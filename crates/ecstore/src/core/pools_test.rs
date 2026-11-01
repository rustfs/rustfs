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

#[cfg(test)]
mod capacity_dedup_tests {
    use crate::core::pools::{
        fallback_free_capacity_dedup, fallback_total_capacity_dedup, get_total_usable_capacity, get_total_usable_capacity_free,
    };

    #[test]
    fn test_single_disk_no_duplication() {
        let disks = vec![rustfs_madmin::Disk {
            endpoint: "node1".to_string(),
            drive_path: "/mnt/disk1".to_string(),
            pool_index: 0,
            set_index: 0,
            disk_index: 0,
            total_space: 2_000_000_000_000, // 2TB
            available_space: 500_000_000_000,
            used_space: 1_500_000_000_000,
            state: "ok".to_string(),
            ..Default::default()
        }];

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: disks.clone(),
            ..Default::default()
        };

        let total = get_total_usable_capacity(&disks, &info);
        let free = get_total_usable_capacity_free(&disks, &info);

        assert_eq!(total, 2_000_000_000_000, "Total capacity should be 2TB");
        assert_eq!(free, 500_000_000_000, "Free capacity should be 500GB");
    }

    #[test]
    fn test_duplicate_disk_entries_deduped() {
        // Simulate the same disk appearing 232 times
        let mut disks = Vec::new();
        for _ in 0..232 {
            disks.push(rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: 2_000_000_000_000,
                available_space: 500_000_000_000,
                used_space: 1_500_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            });
        }

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: disks.clone(),
            ..Default::default()
        };

        let total = get_total_usable_capacity(&disks, &info);
        let free = get_total_usable_capacity_free(&disks, &info);

        // Should only be counted once, not 232 times
        assert_eq!(total, 2_000_000_000_000, "Duplicate disks should be counted only once");
        assert_eq!(free, 500_000_000_000, "Free capacity should not be multiplied");

        // If not deduplicated, the result would be:
        // total = 2TB × 232 = 464TB ❌
    }

    #[test]
    fn test_four_node_ec_2_2_reports_stable_usable_capacity() {
        const TIB: u64 = 1 << 40;
        const DISK_TOTAL: u64 = 2 * TIB;
        const DISK_FREE: u64 = TIB / 5;

        let disks = vec![
            rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/media/rustfs-01".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 0,
                total_space: DISK_TOTAL,
                available_space: DISK_FREE,
                used_space: DISK_TOTAL - DISK_FREE,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node2".to_string(),
                drive_path: "/media/rustfs-01".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 1,
                total_space: DISK_TOTAL,
                available_space: DISK_FREE,
                used_space: DISK_TOTAL - DISK_FREE,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node3".to_string(),
                drive_path: "/media/rustfs-01".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 2,
                total_space: DISK_TOTAL,
                available_space: DISK_FREE,
                used_space: DISK_TOTAL - DISK_FREE,
                state: "ok".to_string(),
                ..Default::default()
            },
            rustfs_madmin::Disk {
                endpoint: "node4".to_string(),
                drive_path: "/media/rustfs-01".to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index: 3,
                total_space: DISK_TOTAL,
                available_space: DISK_FREE,
                used_space: DISK_TOTAL - DISK_FREE,
                state: "ok".to_string(),
                ..Default::default()
            },
        ];

        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![2],   // 2 data disks
                standard_sc_parity: Some(2), // 2 parity disks
                ..Default::default()
            },
            disks: disks.clone(),
            ..Default::default()
        };

        let total = get_total_usable_capacity(&disks, &info);
        let free = get_total_usable_capacity_free(&disks, &info);
        let used = total.saturating_sub(free);
        let expected_total = usize::try_from(4 * TIB).expect("4 TiB must fit the supported platform's usize");
        let expected_free = usize::try_from(2 * DISK_FREE).expect("usable free capacity must fit usize");
        let single_node_used = usize::try_from(DISK_TOTAL - DISK_FREE).expect("single-node used capacity must fit usize");

        assert_eq!(total, expected_total, "usable total must count the two data disks");
        assert_eq!(free, expected_free, "usable free must count the two data disks");
        assert_eq!(used, 2 * single_node_used, "usable used must be approximately 3.6 TiB");
        assert_ne!(used, single_node_used, "used must not collapse to one node's approximately 1.8 TiB");
        assert_ne!(used, 4 * single_node_used, "used must not report the approximately 7.2 TiB raw aggregate");
    }

    #[test]
    fn test_fallback_dedup() {
        // Test deduplication capability of fallback functions
        let mut disks = Vec::new();

        // Add duplicate disks
        for _ in 0..100 {
            disks.push(rustfs_madmin::Disk {
                endpoint: "node1".to_string(),
                drive_path: "/mnt/disk1".to_string(),
                total_space: 2_000_000_000_000,
                available_space: 500_000_000_000,
                state: "ok".to_string(),
                ..Default::default()
            });
        }

        let total = fallback_total_capacity_dedup(&disks);
        let free = fallback_free_capacity_dedup(&disks);

        assert_eq!(total, 2_000_000_000_000);
        assert_eq!(free, 500_000_000_000);
    }
}

#[cfg(all(test, feature = "test-util"))]
mod decommission_lock_order_tests {
    use crate::bucket::lifecycle::lifecycle::TRANSITION_PENDING;
    use crate::core::pools::{
        DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX, DecommissionCapacityLockOrderBarrier, DecommissionCapacityOwner,
        DecommissionErasureLayout, DecommissionPoolCapacityInfo, DecommissionTestFaultGuard, POOL_META_NAME,
        decommission_capacity_mutation_id, decommission_target_gate_busy_error,
        set_decommission_capacity_info_overrides_for_test,
    };
    use crate::data_movement;
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::services::rebalance::{
        test_three_pool_stores_with_isolated_node_contexts,
        test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts,
    };
    use crate::services::tier::test_util::register_mock_tier;
    use crate::set_disk::{
        DeleteObjectCommitBarrier, MultipartCommitBarrier, MultipartCommitPause, NewMultipartUploadCommitObservation,
        PutObjectCommitBarrier, PutObjectCommitPause,
    };
    use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
    use crate::storage_api_contracts::multipart::MultipartOperations as _;
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::object::{ObjectIO, ObjectOperations as _};
    use http::HeaderMap;
    use rustfs_filemeta::MetaCacheEntry;
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::{
        Arc, Condvar, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;

    fn run_large_stack_async_test<C, F>(name: &str, case: C)
    where
        C: FnOnce() -> F + Send + 'static,
        F: Future<Output = ()> + 'static,
    {
        const STACK_SIZE: usize = if cfg!(debug_assertions) {
            8 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else if cfg!(target_os = "macos") {
            2 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else {
            rustfs_config::DEFAULT_THREAD_STACK_SIZE
        };
        std::thread::Builder::new()
            .name(name.to_string())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .thread_stack_size(STACK_SIZE)
                    .build()
                    .expect("large-stack ecstore test runtime should build");
                runtime.block_on(case());
            })
            .expect("large-stack ecstore test thread should spawn")
            .join()
            .expect("large-stack ecstore test thread should complete");
    }

    fn run_large_stack_current_thread_async_test<C, F>(name: &str, case: C)
    where
        C: FnOnce() -> F + Send + 'static,
        F: Future<Output = ()> + 'static,
    {
        const STACK_SIZE: usize = if cfg!(debug_assertions) {
            8 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else if cfg!(target_os = "macos") {
            2 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else {
            rustfs_config::DEFAULT_THREAD_STACK_SIZE
        };
        std::thread::Builder::new()
            .name(name.to_string())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("large-stack current-thread ecstore test runtime should build");
                runtime.block_on(case());
            })
            .expect("large-stack current-thread ecstore test should spawn")
            .join()
            .expect("large-stack current-thread ecstore test should complete");
    }

    #[derive(Clone, Copy)]
    enum ExternalObjectMutation {
        Put,
        CompleteMultipart,
        Copy,
        PutTags,
        DeleteTags,
        PutMetadata,
        Delete,
        Restore,
    }

    impl ExternalObjectMutation {
        fn label(self) -> &'static str {
            match self {
                Self::Put => "put",
                Self::CompleteMultipart => "complete-multipart",
                Self::Copy => "copy",
                Self::PutTags => "put-tags",
                Self::DeleteTags => "delete-tags",
                Self::PutMetadata => "put-metadata",
                Self::Delete => "delete",
                Self::Restore => "restore",
            }
        }
    }

    fn decommission_capacity_owner(meta: &crate::core::pools::PoolMeta) -> DecommissionCapacityOwner {
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("test decommission capacity reservation should exist");
        DecommissionCapacityOwner {
            source_pool_index: 0,
            operation_id: reservation.operation_id,
            generation: reservation.generation,
            owner_nonce: reservation.owner_nonce,
            mutation_id: None,
        }
    }

    fn test_bucket(label: &str) -> String {
        format!("dc-{label}-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
    }

    async fn new_multipart_upload(
        store: &Arc<crate::store::ECStore>,
        pool_index: usize,
        bucket: &str,
        object: &str,
        mut opts: ObjectOptions,
    ) -> crate::error::Result<crate::storage_api_contracts::multipart::MultipartUploadResult> {
        let lifecycle_guard = store.acquire_bucket_lifecycle_read_lock(bucket).await?;
        opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        store.pools[pool_index].new_multipart_upload(bucket, object, &opts).await
    }

    async fn multipart_options(store: &Arc<crate::store::ECStore>, bucket: &str, mut opts: ObjectOptions) -> ObjectOptions {
        let lifecycle_guard = store
            .acquire_bucket_lifecycle_read_lock(bucket)
            .await
            .expect("multipart test should acquire the bucket lifecycle fence");
        opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        opts
    }

    #[derive(Debug)]
    struct CapacityLeaseLossControl {
        calls: AtomicUsize,
        fail_refresh: std::sync::atomic::AtomicBool,
    }

    impl CapacityLeaseLossControl {
        fn arm(&self) {
            self.fail_refresh.store(true, Ordering::Release);
        }

        fn load(&self, ordering: Ordering) -> usize {
            self.calls.load(ordering)
        }
    }

    #[derive(Debug)]
    struct CapacityLeaseLossClient {
        control: Arc<CapacityLeaseLossControl>,
        active: tokio::sync::Mutex<HashMap<rustfs_lock::LockId, rustfs_lock::ObjectKey>>,
    }

    fn is_capacity_lease_resource(resource: &rustfs_lock::ObjectKey) -> bool {
        resource.bucket.as_ref() == RUSTFS_META_BUCKET
            && (resource.object.as_ref() == POOL_META_NAME
                || resource
                    .object
                    .as_ref()
                    .strip_prefix(DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX)
                    .is_some_and(|suffix| suffix.starts_with('/')))
    }

    #[async_trait::async_trait]
    impl rustfs_lock::LockClient for CapacityLeaseLossClient {
        async fn acquire_lock(&self, request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<rustfs_lock::LockResponse> {
            self.active
                .lock()
                .await
                .insert(request.lock_id.clone(), request.resource.clone());
            let now = std::time::SystemTime::now();
            Ok(rustfs_lock::LockResponse::success(
                rustfs_lock::LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: rustfs_lock::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: now,
                    expires_at: now + request.ttl,
                    last_refreshed: now,
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        }

        async fn release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id).is_some())
        }

        async fn refresh(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            let resource = self.active.lock().await.get(lock_id).cloned();
            if resource.as_ref().is_some_and(is_capacity_lease_resource) {
                self.control.calls.fetch_add(1, Ordering::Release);
                return Ok(!self.control.fail_refresh.load(Ordering::Acquire));
            }
            Ok(resource.is_some())
        }

        async fn force_release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.release(lock_id).await
        }

        async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<rustfs_lock::LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<rustfs_lock::LockStats> {
            Ok(rustfs_lock::LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    async fn store_with_capacity_lease_loss(
        other_store: &Arc<crate::store::ECStore>,
    ) -> (Arc<crate::store::ECStore>, Arc<CapacityLeaseLossControl>) {
        let mut pool = (*other_store.pools[0]).clone();
        let mut set = (*pool.disk_set[0]).clone();
        let refresh_calls = Arc::new(CapacityLeaseLossControl {
            calls: AtomicUsize::new(0),
            fail_refresh: std::sync::atomic::AtomicBool::new(false),
        });
        set.lockers = (0..set.lockers.len().max(1))
            .map(|_| {
                Arc::new(CapacityLeaseLossClient {
                    control: Arc::clone(&refresh_calls),
                    active: tokio::sync::Mutex::new(HashMap::new()),
                }) as Arc<dyn rustfs_lock::LockClient>
            })
            .collect();
        pool.disk_set[0] = Arc::new(set);
        let mut pools = other_store.pools.clone();
        pools[0] = Arc::new(pool);
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        ctx.update_erasure_type(crate::layout::endpoints::SetupType::DistErasure)
            .await;
        let endpoints = other_store
            .ctx
            .endpoints()
            .expect("lease-loss test store should have endpoint topology");
        ctx.set_endpoints(endpoints.clone());
        if let Some(deployment_id) = other_store.ctx.deployment_id() {
            ctx.set_deployment_id(deployment_id);
        }
        let authority = ctx
            .bind_multipart_proof_topology(&endpoints)
            .expect("lease-loss fixture must own its complete native proof topology");
        let mut local_disk_map = HashMap::new();
        for pool in &mut pools {
            let pool = Arc::make_mut(pool);
            pool.set_instance_ctx_for_test(Arc::clone(&ctx));
            for set in &mut pool.disk_set {
                let set = Arc::make_mut(set);
                let source_disks = set.disks.read().await.clone();
                let mut disks = Vec::with_capacity(set.set_endpoints.len());
                for (slot, endpoint) in set.set_endpoints.iter().enumerate() {
                    // The logical node clone shares durable contents, never a
                    // native handle already owned by another instance scope.
                    let disk = crate::disk::new_disk(
                        endpoint,
                        &crate::disk::DiskOption {
                            cleanup: false,
                            health_check: false,
                        },
                    )
                    .await
                    .expect("open the lease-loss node's independently owned native handle");
                    assert!(!Arc::ptr_eq(&disk, source_disks[slot].as_ref().expect("source fixture disk")));
                    assert_eq!(
                        crate::disk::DiskAPI::get_disk_id(disk.as_ref()).await.unwrap(),
                        Some(pool.format.erasure.sets[set.set_index][slot])
                    );
                    local_disk_map.insert(endpoint.to_string(), Some(disk.clone()));
                    disks.push(Some(disk));
                }
                crate::disk::multipart_source::bind_multipart_proof_snapshot(&disks, &authority)
                    .expect("lease-loss native handles must bind only to this fixture instance");
                set.disks = Arc::new(tokio::sync::RwLock::new(disks));
            }
        }
        *ctx.local_disk_map().write().await = local_disk_map;
        let store = Arc::new(crate::store::ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: other_store.disk_map.clone(),
            pools,
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoints, Arc::clone(&ctx)),
            pool_meta: tokio::sync::RwLock::new(other_store.pool_meta.read().await.clone()),
            rebalance_meta: tokio::sync::RwLock::new(other_store.rebalance_meta.read().await.clone()),
            decommission_cancelers: tokio::sync::RwLock::new(vec![None; other_store.pools.len()]),
            start_gate: tokio::sync::Mutex::new(()),
            pool_meta_save_gate: tokio::sync::Mutex::new(
                other_store.pool_meta_save_gate.lock().await.independent_clone_for_test(),
            ),
            ctx,
            bucket_fence_registry: Arc::default(),
        });
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        (store, refresh_calls)
    }

    async fn assert_lock_order(mutation: ExternalObjectMutation) {
        assert_lock_order_with_tail(mutation, false).await;
    }

    /// Leave inflight bytes on the active decommission's allocation for
    /// `target_pool_index` that no mutation will settle, as a lost marker
    /// would, and persist them.
    async fn strand_decommission_target_marker(store: &Arc<crate::store::ECStore>, target_pool_index: usize, bytes: usize) {
        let mut meta = store.pool_meta.read().await.clone();
        let reservation = meta.pools[0]
            .decommission
            .as_mut()
            .and_then(|info| info.capacity_reservation.as_mut())
            .expect("the decommission reservation should be active");
        let target = reservation
            .targets
            .iter_mut()
            .find(|target| target.pool_index == target_pool_index)
            .expect("the reservation should allocate the target pool");
        target
            .temporary_mutations
            .push(crate::core::pools::DecommissionCapacityTemporaryMutation {
                mutation_id: uuid::Uuid::new_v4(),
                physical_bytes: bytes,
            });
        target.inflight_physical_bytes += bytes;
        target.observed_physical_bytes += bytes;
        reservation.inflight_target_physical_bytes += bytes;
        reservation.observed_target_physical_bytes += bytes;
        meta.save(store.pools.clone())
            .await
            .expect("persist the stranded target marker");
        *store.pool_meta.write().await = meta;
    }

    /// Start a decommission of `bucket` on a three-pool store where target 1
    /// holds most of the allocation and `stranded` inflight bytes of a marker
    /// no mutation will settle.
    async fn start_over_budget_routine_decommission(store: &Arc<crate::store::ECStore>, bucket: &str, stranded: usize) {
        // Of the 128 MiB peak, target 1 takes 100 MiB and target 2 the other
        // 28 MiB; target 1's temporary-copy budget is 50 MiB.
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        start_routine_decommission_with_capacity(
            store,
            bucket,
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
            ],
            stranded,
        )
        .await;
    }

    async fn start_routine_decommission_with_capacity(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        capacity: Vec<DecommissionPoolCapacityInfo>,
        stranded: usize,
    ) {
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity]);
        store
            .save_current_pool_meta_for_decommission_start(
                &[0],
                vec![crate::core::pools::DecomBucketInfo {
                    name: bucket.to_string(),
                    prefix: String::new(),
                }],
            )
            .await
            .expect("activate the decommission");
        if stranded > 0 {
            strand_decommission_target_marker(store, 1, stranded).await;
        }
    }

    /// Pause one of `object`'s own target-1 gate acquisitions, after `skips`
    /// of them, by arming the pause when the routine reads that object.
    fn pause_object_target_gate_acquire(
        barrier: Arc<DecommissionCapacityLockOrderBarrier>,
        bucket: String,
        object: String,
        skips: usize,
    ) -> DecommissionTestFaultGuard {
        let armed = Arc::new(AtomicBool::new(false));
        DecommissionTestFaultGuard::install(Arc::new(move |stage, entry_bucket, entry_object, _, _| {
            if stage == "object_read" && entry_bucket == bucket && entry_object == object && !armed.swap(true, Ordering::AcqRel) {
                barrier.pause_one_target_gate_acquire_after(1, skips);
            }
            false
        }))
    }

    /// Another mutation commits `bytes` on target 1, as a concurrent entry
    /// would.
    async fn commit_other_mutation_on_target_1(store: &Arc<crate::store::ECStore>, bytes: usize) {
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        store
            .run_decommission_capacity_admitted_mutation(1, Some(owner), Some(bytes), || async {
                Ok::<_, crate::error::Error>(())
            })
            .await
            .expect("the other mutation commits on target 1");
    }

    async fn assert_target_1_over_budget(store: &Arc<crate::store::ECStore>) {
        let target = killed_leader_target(store, 1).await;
        let budget = target
            .reserved_physical_bytes
            .saturating_sub(target.consumed_physical_bytes * 2)
            / 2;
        assert!(
            target.inflight_physical_bytes > budget,
            "target 1 must be over its {budget}-byte budget: {target:?}"
        );
    }

    fn spawn_decommission_routine(
        store: &Arc<crate::store::ECStore>,
        entry_budget: usize,
    ) -> tokio::task::JoinHandle<crate::error::Result<()>> {
        tokio::spawn({
            let store = Arc::clone(store);
            async move {
                let canceler = crate::core::pools::DecommissionCanceler::new(CancellationToken::new());
                store.decommission_cancelers.write().await[0] = Some(canceler.clone());
                store
                    .do_decommission_in_routine(canceler, 0, Arc::new(tokio::sync::Semaphore::new(entry_budget)))
                    .await
            }
        })
    }

    async fn run_routine_over_a_target_going_over_budget(
        store: &Arc<crate::store::ECStore>,
        barrier: &DecommissionCapacityLockOrderBarrier,
        other_commit_bytes: usize,
        before_other_commit: impl std::future::Future<Output = ()>,
    ) {
        run_routine_while_target_1_changes(store, barrier, 2, other_commit_bytes, true, before_other_commit).await;
    }

    /// Run the routine while one entry, held between its target selection and
    /// target 1's gate, waits for another mutation to commit on target 1, and
    /// expect the routine to complete without a pause or a failed item.
    async fn run_routine_while_target_1_changes(
        store: &Arc<crate::store::ECStore>,
        barrier: &DecommissionCapacityLockOrderBarrier,
        entry_budget: usize,
        other_commit_bytes: usize,
        expect_over_budget: bool,
        before_other_commit: impl std::future::Future<Output = ()>,
    ) {
        let routine = spawn_decommission_routine(store, entry_budget);
        // The object selected target 1 and waits to acquire its gate.
        barrier.wait_until_target_gate_acquire_paused().await;
        before_other_commit.await;
        commit_other_mutation_on_target_1(store, other_commit_bytes).await;
        if expect_over_budget {
            assert_target_1_over_budget(store).await;
        }
        barrier.release_target_gate_acquire();
        routine
            .await
            .expect("the decommission routine task should join")
            .expect("the decommission routine should return");
        let local = store.pool_meta.read().await;
        let info = local.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert!(
            info.complete && !info.failed && !info.canceled && info.capacity_blocked_reason.is_none(),
            "a target selection that went stale must not pause or fail the decommission: complete={}, failed={}, canceled={}, blocked_reason={:?}",
            info.complete,
            info.failed,
            info.canceled,
            info.capacity_blocked_reason
        );
        assert_eq!(info.items_decommission_failed, 0);
    }

    #[test]
    #[serial_test::serial]
    fn decommission_reselects_a_target_that_went_over_budget_after_selection() {
        run_large_stack_async_test("decommission-reselect-over-budget-target", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("reselect-over-budget").await;
            let object = "late.bin";
            put_source_object(&store, &bucket, object, b"selected target 1 before it went over budget".to_vec()).await;
            // 45 of target 1's 50 MiB budget are stranded.
            start_over_budget_routine_decommission(&store, &bucket, 45 << 20).await;
            let barrier = stale_target_barrier(&store);
            let _fault = pause_object_target_gate_acquire(Arc::clone(&barrier), bucket.clone(), object.to_string(), 0);

            // A 6 MiB commit shrinks target 1's budget to 44 MiB.
            run_routine_over_a_target_going_over_budget(
                &store,
                &barrier,
                6 << 20,
                assert_key_not_yet_migrated(&store, &bucket, object),
            )
            .await;
            store.pools[2]
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("the entry must be placed on target 2 after target 1 went over budget");
            assert!(
                store.pools[1]
                    .get_object_info(&bucket, object, &ObjectOptions::default())
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_restarts_a_multipart_upload_pinned_to_a_target_that_went_over_budget() {
        run_large_stack_async_test("decommission-reselect-over-budget-multipart", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("reselect-over-budget-mpu").await;
            let object = "late-multipart.bin";
            put_source_multipart_object(&store, &bucket, object).await;
            // Staged parts record the bytes they reserved, as on real disks.
            crate::core::pools::observe_expected_decommission_target_bytes_for_test(store.id);
            // 40 of target 1's 50 MiB budget are stranded.
            start_over_budget_routine_decommission(&store, &bucket, 40 << 20).await;
            let barrier = stale_target_barrier(&store);
            // Skip the upload creation and part 1; pause before part 2.
            let _fault = pause_object_target_gate_acquire(Arc::clone(&barrier), bucket.clone(), object.to_string(), 2);

            // A 12 MiB commit shrinks target 1's budget to 38 MiB while the
            // upload, pinned to target 1, waits to upload part 2.
            run_routine_over_a_target_going_over_budget(
                &store,
                &barrier,
                12 << 20,
                assert_target_1_inflight_while_held(&store, &bucket, object, 45 << 20),
            )
            .await;
            assert_eq!(
                killed_leader_target(&store, 1).await.inflight_physical_bytes,
                40 << 20,
                "cleaning up the pinned upload must release its staged bytes on target 1"
            );
            let migrated = store.pools[2]
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("the restarted upload must complete on target 2");
            assert!(migrated.is_multipart());
            assert!(
                store.pools[1]
                    .get_object_info(&bucket, object, &ObjectOptions::default())
                    .await
                    .is_err()
            );
        });
    }

    /// A two-part source object: a 5 MiB part and a 1-byte part.
    async fn put_source_multipart_object(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
    ) -> crate::object_api::ObjectInfo {
        let source_upload = new_multipart_upload(store, 0, bucket, object, ObjectOptions::default())
            .await
            .expect("create the source multipart upload");
        let source_opts = multipart_options(store, bucket, ObjectOptions::default()).await;
        let mut source_parts = Vec::with_capacity(2);
        for (part_num, part_body) in [(1, vec![0x31; 5 << 20]), (2, vec![0x32; 1])] {
            let part = store.pools[0]
                .put_object_part(
                    bucket,
                    object,
                    &source_upload.upload_id,
                    part_num,
                    &mut PutObjReader::from_vec(part_body),
                    &source_opts,
                )
                .await
                .expect("stage the source multipart part");
            source_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        let source_info = store.pools[0]
            .clone()
            .complete_multipart_upload(bucket, object, &source_upload.upload_id, source_parts, &source_opts)
            .await
            .expect("complete the source multipart object");
        assert!(source_info.is_multipart());
        source_info
    }

    async fn put_source_object(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str, body: Vec<u8>) {
        store.pools[0]
            .put_object(bucket, object, &mut PutObjReader::from_vec(body), &ObjectOptions::default())
            .await
            .expect("write the source object");
    }

    /// A key that sorts after `anchor` in the same source set. Listing order
    /// does not guarantee execution order between the set's entry workers.
    fn source_key_after(store: &Arc<crate::store::ECStore>, anchor: &str, label: &str) -> String {
        let anchor_set = store.pools[0].get_disks_by_key(anchor).set_index;
        (0..1024)
            .map(|candidate| format!("b-{label}-{candidate}.bin"))
            .find(|candidate| store.pools[0].get_disks_by_key(candidate).set_index == anchor_set)
            .expect("some candidate key should share the anchor's source set")
    }

    /// Transition a versioned source object to a mock tier. With
    /// `free_version`, also delete the transitioned version so that only its
    /// tier free version is left to migrate.
    async fn seed_source_transitioned_object(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        free_version: bool,
    ) -> impl Sized {
        let tier_name = format!("DECOMSTALE{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
        let source = store.pools[0]
            .put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(b"transitioned source bytes".to_vec()),
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write the transitioned source");
        let version_id = source.version_id.map(|version| version.to_string());
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: version_id.clone(),
                    transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: source.etag.clone().expect("the transitioned source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("transition the source before the decommission");
        if free_version {
            store.pools[0]
                .delete_object(
                    bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        version_id,
                        ..Default::default()
                    },
                )
                .await
                .expect("delete the transitioned version");
            let versions = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("the source versions should decode")
                .expect("the free version should remain");
            assert!(versions.versions.iter().any(|version| version.tier_free_version()));
        }
        backend
    }

    async fn new_stale_target_store(label: &str) -> (Vec<tempfile::TempDir>, Arc<crate::store::ECStore>, String) {
        let (temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket(label);
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the stale-target bucket");
        (temp_dirs, store, bucket)
    }

    fn stale_target_barrier(store: &Arc<crate::store::ECStore>) -> Arc<DecommissionCapacityLockOrderBarrier> {
        let barrier = Arc::new(DecommissionCapacityLockOrderBarrier::install(store.id, store.id));
        barrier.disable_owner_pause();
        barrier
    }

    async fn assert_key_not_yet_migrated(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str) {
        for target_pool_index in [1, 2] {
            let versions = store.pools[target_pool_index]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("the target versions should decode");
            assert!(
                versions.is_none_or(|versions| versions.versions.is_empty()),
                "{object} must not be on target {target_pool_index} while it is held before its gate"
            );
        }
    }

    /// The held entry has not migrated, and target 1 holds exactly
    /// `inflight` bytes: the stranded marker plus what the entry staged.
    async fn assert_target_1_inflight_while_held(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        inflight: usize,
    ) {
        assert_key_not_yet_migrated(store, bucket, object).await;
        let target = killed_leader_target(store, 1).await;
        assert_eq!(
            target.inflight_physical_bytes, inflight,
            "unexpected staged bytes on target 1 at the pause: {target:?}"
        );
    }

    async fn assert_source_key_migrated_to_target_2(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str) {
        let versions = store.pools[2]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("the target 2 versions should decode");
        assert!(
            versions.is_some_and(|versions| !versions.versions.is_empty()),
            "{object} must be migrated to target 2"
        );
        let on_target_1 = store.pools[1]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("the target 1 versions should decode");
        assert!(
            on_target_1.is_none_or(|versions| versions.versions.is_empty()),
            "{object} must not land on target 1"
        );
    }

    /// A source cleanup that keeps losing quorum through its in-place retries
    /// is deferred, replayed at the end of the round and completed. The source
    /// is removed, the bucket finishes, and the entry's version is counted once
    /// even though the replay walks it again.
    #[test]
    #[serial_test::serial]
    fn decommission_defers_cleanup_quorum_failure_and_completes_on_replay() {
        run_large_stack_async_test("decommission-cleanup-quorum-replay", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("cleanup-quorum-replay").await;
            let object = "cleanup-quorum.bin";
            // Large enough that the small `.rustfs.sys` objects the routine also
            // moves cannot account for a second count of it.
            let object_size = 1 << 20;
            put_source_object(&store, &bucket, object, vec![0x5c; object_size]).await;

            // Every in-place attempt of the first pass fails; the replay succeeds.
            let failing_calls = crate::core::pools::DECOMMISSION_SOURCE_CLEANUP_QUORUM_MAX_ATTEMPTS;
            let cleanup_calls = Arc::new(AtomicUsize::new(0));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let cleanup_calls = Arc::clone(&cleanup_calls);
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "source_cleanup"
                            && entry_bucket == bucket
                            && entry_object == object
                            && cleanup_calls.fetch_add(1, Ordering::AcqRel) < failing_calls
                    }
                }),
                Some(Arc::new(|_, _, _| crate::error::Error::ErasureWriteQuorum)),
            );

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            start_routine_decommission_with_capacity(
                &store,
                &bucket,
                vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 1 << 30, 1 << 30, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
                ],
                0,
            )
            .await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission routine should return");

            assert_eq!(
                cleanup_calls.load(Ordering::Acquire),
                failing_calls + 1,
                "the first pass exhausts its in-place retries and the replay cleans up once"
            );
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled && info.capacity_blocked_reason.is_none(),
                "a deferred cleanup that succeeds on replay must complete the decommission: complete={}, failed={}, canceled={}, blocked_reason={:?}",
                info.complete,
                info.failed,
                info.canceled,
                info.capacity_blocked_reason
            );
            assert!(!info.queued_buckets.contains(&bucket), "the bucket must be popped: {info:?}");
            assert!(
                info.bytes_done >= object_size && info.bytes_done < 2 * object_size,
                "the replay must not count the version again: bytes_done={}",
                info.bytes_done
            );
            assert_eq!(info.items_decommission_failed, 0);
            drop(local);

            let source = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("the source versions should decode");
            assert!(
                source.is_none_or(|versions| versions.versions.is_empty()),
                "the replayed cleanup must remove the source"
            );
            let mut on_targets = 0;
            for target_pool_index in [1, 2] {
                let target = store.pools[target_pool_index]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(&bucket, object)
                    .await
                    .expect("the target versions should decode");
                if target.is_some_and(|versions| !versions.versions.is_empty()) {
                    on_targets += 1;
                }
            }
            assert_eq!(on_targets, 1, "the object must live on exactly one target pool");
        });
    }

    /// A source cleanup that keeps losing quorum through the replay pauses the
    /// decommission instead of failing it, with a reason that names source
    /// cleanup rather than target capacity, and leaves the source in place.
    #[test]
    #[serial_test::serial]
    fn decommission_pauses_on_persistent_cleanup_quorum_failure_as_source_cleanup() {
        run_large_stack_async_test("decommission-cleanup-quorum-pause", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("cleanup-quorum-pause").await;
            let object = "cleanup-quorum-pause.bin";
            put_source_object(&store, &bucket, object, b"source cleanup never regains quorum".to_vec()).await;

            let cleanup_calls = Arc::new(AtomicUsize::new(0));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let cleanup_calls = Arc::clone(&cleanup_calls);
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        let hit = stage == "source_cleanup" && entry_bucket == bucket && entry_object == object;
                        if hit {
                            cleanup_calls.fetch_add(1, Ordering::AcqRel);
                        }
                        hit
                    }
                }),
                Some(Arc::new(|_, _, _| crate::error::Error::ErasureWriteQuorum)),
            );

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            start_routine_decommission_with_capacity(
                &store,
                &bucket,
                vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 1 << 30, 1 << 30, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
                ],
                0,
            )
            .await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("a paused decommission routine returns without an error");

            assert_eq!(
                cleanup_calls.load(Ordering::Acquire),
                2 * crate::core::pools::DECOMMISSION_SOURCE_CLEANUP_QUORUM_MAX_ATTEMPTS,
                "the first pass and the replay each exhaust their in-place retries"
            );
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                !info.complete && !info.failed && !info.canceled,
                "a persistent cleanup failure must pause, not finish or fail: complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert!(info.is_source_cleanup_blocked(), "{:?}", info.capacity_blocked_reason);
            let reason = info.capacity_blocked_reason.as_deref().unwrap_or_default();
            assert!(reason.contains("decommission cleanup_delete_object failed for"), "{reason}");
            assert!(!reason.contains("free drive threshold"), "{reason}");
            assert!(info.queued_buckets.contains(&bucket), "the bucket must not be marked done: {info:?}");
            drop(local);

            let source = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("the source versions should decode");
            assert!(
                source.is_some_and(|versions| !versions.versions.is_empty()),
                "the source must stay in place while cleanup cannot commit"
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_reselects_a_target_that_no_longer_fits_after_selection() {
        run_large_stack_async_test("decommission-reselect-no-longer-fits", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("reselect-no-longer-fits").await;
            let object = "late-large.bin";
            put_source_object(&store, &bucket, object, vec![0x33; 30 << 20]).await;
            // Of the 180 MiB peak, target 1 reserves 100 MiB and target 2 the
            // other 80; a 30 MiB object needs 60 MiB with its temporary copy.
            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            start_routine_decommission_with_capacity(
                &store,
                &bucket,
                vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 90 << 20, 90 << 20),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 80 << 20, 80 << 20, 0),
                ],
                0,
            )
            .await;
            let barrier = stale_target_barrier(&store);
            let _fault = pause_object_target_gate_acquire(Arc::clone(&barrier), bucket.clone(), object.to_string(), 0);

            // A 25 MiB commit leaves target 1 50 MiB, within budget but too
            // little for the object.
            run_routine_while_target_1_changes(
                &store,
                &barrier,
                2,
                25 << 20,
                false,
                assert_key_not_yet_migrated(&store, &bucket, object),
            )
            .await;
            assert_source_key_migrated_to_target_2(&store, &bucket, object).await;
        });
    }

    /// `staged` is what the held upload has staged on target 1 at the pause.
    async fn run_stale_multipart_case(label: &str, skips: usize, stranded: usize, staged: usize, other_commit_bytes: usize) {
        let (_temp_dirs, store, bucket) = new_stale_target_store(label).await;
        let object = "late-multipart.bin";
        put_source_multipart_object(&store, &bucket, object).await;
        crate::core::pools::observe_expected_decommission_target_bytes_for_test(store.id);
        start_over_budget_routine_decommission(&store, &bucket, stranded).await;
        let barrier = stale_target_barrier(&store);
        let _fault = pause_object_target_gate_acquire(Arc::clone(&barrier), bucket.clone(), object.to_string(), skips);
        run_routine_while_target_1_changes(
            &store,
            &barrier,
            2,
            other_commit_bytes,
            true,
            assert_target_1_inflight_while_held(&store, &bucket, object, stranded + staged),
        )
        .await;
        assert_source_key_migrated_to_target_2(&store, &bucket, object).await;
    }

    #[test]
    #[serial_test::serial]
    fn decommission_reselects_when_the_upload_target_went_over_budget() {
        // Hold the upload creation; 45 MiB stranded and a 6 MiB commit put
        // target 1 over its 44 MiB budget.
        run_large_stack_async_test("decommission-stale-new-upload", || {
            run_stale_multipart_case("stale-new-upload", 0, 45 << 20, 0, 6 << 20)
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_restarts_an_upload_whose_completion_target_went_over_budget() {
        // Hold the completion after both parts (5 MiB and 1 byte staged); 40
        // MiB stranded and a 12 MiB commit put target 1 over its 38 MiB budget.
        run_large_stack_async_test("decommission-stale-completion", || {
            run_stale_multipart_case("stale-completion", 3, 40 << 20, (5 << 20) + 1, 12 << 20)
        });
    }

    /// Pause the subject's first target mutation after its exact versions are
    /// loaded, without relying on another entry's execution order.
    async fn run_stale_subject_case(store: &Arc<crate::store::ECStore>, bucket: &str, subject: &str, expected_versions: usize) {
        start_over_budget_routine_decommission(store, bucket, 45 << 20).await;
        let barrier = stale_target_barrier(store);
        let _fault = {
            let barrier = Arc::clone(&barrier);
            let bucket = bucket.to_string();
            let subject = subject.to_string();
            let armed = AtomicBool::new(false);
            DecommissionTestFaultGuard::install(Arc::new(move |stage, entry_bucket, entry_object, _, _| {
                if stage == "exact_versions_read"
                    && entry_bucket == bucket
                    && entry_object == subject
                    && !armed.swap(true, Ordering::AcqRel)
                {
                    barrier.pause_one_target_gate_acquire_after(1, 0);
                }
                false
            }))
        };
        let mut items_before_subject = 0;
        run_routine_while_target_1_changes(store, &barrier, 1, 6 << 20, true, async {
            assert_held_at_subject(store, bucket, subject, expected_versions).await;
            items_before_subject = store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present")
                .items_decommissioned;
        })
        .await;
        assert_source_key_migrated_to_target_2(store, bucket, subject).await;
        assert_eq!(
            store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present")
                .items_decommissioned,
            items_before_subject + expected_versions,
            "each subject version must be counted once after target reselection"
        );
    }

    /// The subject's exact source versions remain, and neither target has it.
    async fn assert_held_at_subject(store: &Arc<crate::store::ECStore>, bucket: &str, subject: &str, expected_versions: usize) {
        let versions = store.pools[0]
            .get_disks_by_key(subject)
            .load_file_info_versions_exact(bucket, subject)
            .await
            .expect("the source versions should decode while the subject is held")
            .expect("the source must remain while the subject is held");
        assert_eq!(versions.versions.len(), expected_versions, "the source versions must remain intact");
        assert_key_not_yet_migrated(store, bucket, subject).await;
    }

    #[test]
    #[serial_test::serial]
    fn decommission_reselects_a_delete_marker_target_that_went_over_budget() {
        run_large_stack_async_test("decommission-stale-delete-marker", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("stale-delete-marker").await;
            let subject = "marker.bin";
            store.pools[0]
                .put_object(
                    &bucket,
                    subject,
                    &mut PutObjReader::from_vec(b"marked version".to_vec()),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the marked version");
            let marker = store.pools[0]
                .delete_object(
                    &bucket,
                    subject,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the delete marker");
            assert!(marker.delete_marker);
            run_stale_subject_case(&store, &bucket, subject, 2).await;
            let versions = store.pools[2]
                .get_disks_by_key(subject)
                .load_file_info_versions_exact(&bucket, subject)
                .await
                .expect("the target 2 versions should decode")
                .expect("the subject should be on target 2");
            assert!(versions.versions.iter().any(|version| version.deleted));
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_reselects_a_tiered_object_target_that_went_over_budget() {
        run_large_stack_async_test("decommission-stale-tiered", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("stale-tiered").await;
            let subject = "tiered.bin";
            let _tier = seed_source_transitioned_object(&store, &bucket, subject, false).await;
            run_stale_subject_case(&store, &bucket, subject, 1).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_counts_a_free_version_once_after_its_target_went_over_budget() {
        run_large_stack_async_test("decommission-stale-free-version", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("stale-free-version").await;
            let subject = "free.bin";
            let _tier = seed_source_transitioned_object(&store, &bucket, subject, true).await;
            run_stale_subject_case(&store, &bucket, subject, 1).await;
            let versions = store.pools[2]
                .get_disks_by_key(subject)
                .load_file_info_versions_exact(&bucket, subject)
                .await
                .expect("the target 2 versions should decode")
                .expect("the free version should be on target 2");
            assert!(versions.versions.iter().any(|version| version.tier_free_version()));
        });
    }

    /// Edit target 1's ledger and persist it, as a write or a reconciliation
    /// the test does not run would.
    async fn edit_target_1_ledger(
        store: &Arc<crate::store::ECStore>,
        edit: impl FnOnce(&mut crate::core::pools::DecommissionCapacityReservation),
    ) {
        let mut meta = store.pool_meta.read().await.clone();
        edit(
            meta.pools[0]
                .decommission
                .as_mut()
                .and_then(|info| info.capacity_reservation.as_mut())
                .expect("the decommission reservation should be active"),
        );
        meta.save(store.pools.clone())
            .await
            .expect("persist the edited target ledger");
        *store.pool_meta.write().await = meta;
    }

    #[test]
    #[serial_test::serial]
    fn decommission_counts_a_free_version_once_after_an_intent_conflict() {
        run_large_stack_async_test("decommission-free-version-intent-conflict", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("free-version-conflict").await;
            let subject = "free-conflict.bin";
            let _tier = seed_source_transitioned_object(&store, &bucket, subject, true).await;
            start_over_budget_routine_decommission(&store, &bucket, 0).await;
            let barrier = stale_target_barrier(&store);
            let _fault = {
                let barrier = Arc::clone(&barrier);
                let bucket = bucket.clone();
                DecommissionTestFaultGuard::install(Arc::new(move |stage, entry_bucket, entry_object, _, _| {
                    if stage == "exact_versions_read" && entry_bucket == bucket && entry_object == subject {
                        barrier.pause_one_target_gate_acquire_after(1, 0);
                    }
                    false
                }))
            };
            let routine = spawn_decommission_routine(&store, 1);

            // The free version selected target 1. Another mutation fails after
            // persisting its intent there, so the free version's admission
            // meets an intent conflict; hold its retry as well.
            barrier.wait_until_target_gate_acquire_paused().await;
            assert_held_at_subject(&store, &bucket, subject, 1).await;
            let items_before_subject = store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present")
                .items_decommissioned;
            let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
            store
                .run_decommission_capacity_admitted_mutation(1, Some(owner), Some(1 << 20), || async {
                    Err::<(), _>(crate::error::Error::other("the other mutation fails after its intent"))
                })
                .await
                .expect_err("the other mutation must fail");
            assert!(killed_leader_target(&store, 1).await.pending_mutation_id.is_some());
            // The conflicting attempt reloads the subject and arms its own retry.
            barrier.release_target_gate_acquire();

            // Resolve the intent while that retry is held before its gate.
            barrier.wait_until_target_gate_acquire_paused().await;
            assert_held_at_subject(&store, &bucket, subject, 1).await;
            edit_target_1_ledger(&store, |reservation| {
                let target = reservation
                    .targets
                    .iter_mut()
                    .find(|target| target.pool_index == 1)
                    .expect("the reservation should allocate target 1");
                reservation.pending_target_physical_bytes -= target.pending_physical_bytes;
                target.pending_physical_bytes = 0;
                target.pending_mutation_id = None;
            })
            .await;
            barrier.release_target_gate_acquire();
            routine
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission routine should return");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete
                    && !info.failed
                    && !info.canceled
                    && info.capacity_blocked_reason.is_none()
                    && info.items_decommission_failed == 0,
                "a free version retried after an intent conflict must count once, as decommissioned: complete={}, failed={}, items_decommission_failed={}, blocked_reason={:?}",
                info.complete,
                info.failed,
                info.items_decommission_failed,
                info.capacity_blocked_reason
            );
            assert_eq!(
                info.items_decommissioned,
                items_before_subject + 1,
                "the free version must be counted once"
            );
            drop(local);
            let versions = store.pools[1]
                .get_disks_by_key(subject)
                .load_file_info_versions_exact(&bucket, subject)
                .await
                .expect("the target 1 versions should decode")
                .expect("the free version should be on target 1");
            assert_eq!(versions.versions.len(), 1);
            assert!(versions.versions[0].tier_free_version());
            for pool_index in [0, 2] {
                let versions = store.pools[pool_index]
                    .get_disks_by_key(subject)
                    .load_file_info_versions_exact(&bucket, subject)
                    .await
                    .expect("the remaining pool versions should decode");
                assert!(
                    versions.is_none_or(|versions| versions.versions.is_empty()),
                    "the free version must only remain on target 1"
                );
            }
        });
    }

    /// An earlier attempt copied `object` to target 1, but its source cleanup
    /// never ran, so the object stays pinned to target 1.
    async fn copy_source_object_to_target_1(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str) {
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("load the pinned bucket incarnation");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let source_reader = store.pools[0]
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the source object");
        data_movement::migrate_decommission_object(
            Arc::clone(store),
            0,
            bucket.to_string(),
            source_reader,
            Some(incarnation),
            "pinned_copy",
            Some(owner),
        )
        .await
        .expect("copy the object to target 1");
        store.pools[1]
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("the earlier copy should be on target 1");
    }

    #[test]
    #[serial_test::serial]
    fn decommission_defers_an_object_pinned_to_an_over_budget_target() {
        run_large_stack_async_test("decommission-pinned-over-budget", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("pinned-over-budget").await;
            let object = "pinned.bin";
            put_source_object(&store, &bucket, object, b"already copied to target 1".to_vec()).await;
            start_over_budget_routine_decommission(&store, &bucket, 0).await;
            copy_source_object_to_target_1(&store, &bucket, object).await;
            // Target 1 then goes over its 50 MiB budget.
            strand_decommission_target_marker(&store, 1, 60 << 20).await;

            run_routine_to_a_deferral_pause(&store, 1, "target pool 1 is over a temporary-copy budget").await;
        });
    }

    /// Run the routine to its end and expect it to have deferred an entry and
    /// paused, without a failed item, with a reason that names `cause`.
    async fn run_routine_to_a_deferral_pause(store: &Arc<crate::store::ECStore>, entry_budget: usize, cause: &str) {
        spawn_decommission_routine(store, entry_budget)
            .await
            .expect("the decommission routine task should join")
            .expect("the decommission routine should return");
        let local = store.pool_meta.read().await;
        let info = local.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert!(
            !info.complete
                && !info.failed
                && info.items_decommission_failed == 0
                && info
                    .capacity_blocked_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("deferred entries remaining") && reason.contains(cause)),
            "the entry must be deferred and pause on {cause:?}, never fail: complete={}, failed={}, items_decommission_failed={}, blocked_reason={:?}",
            info.complete,
            info.failed,
            info.items_decommission_failed,
            info.capacity_blocked_reason
        );
    }

    #[test]
    #[serial_test::serial]
    fn decommission_defers_a_part_that_no_longer_fits_the_temporary_headroom() {
        run_large_stack_async_test("decommission-temporary-headroom", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("temporary-headroom").await;
            put_source_multipart_object(&store, &bucket, "headroom.bin").await;
            // 46 of target 1's 50 MiB budget are stranded: target 1 is not over
            // budget, so selection keeps choosing it, but the 5 MiB part does
            // not fit the 4 MiB of headroom left.
            start_over_budget_routine_decommission(&store, &bucket, 46 << 20).await;
            run_routine_to_a_deferral_pause(
                &store,
                1,
                "target pool 1 no longer fits the selected mutation: source pool 0 target pool 1 temporary operation requires",
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_names_the_cause_of_a_pinned_object_that_no_longer_fits() {
        run_large_stack_async_test("decommission-pinned-no-longer-fits", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("pinned-no-longer-fits").await;
            let object = "pinned-large.bin";
            put_source_object(&store, &bucket, object, vec![0x34; 4 << 20]).await;
            start_over_budget_routine_decommission(&store, &bucket, 0).await;
            copy_source_object_to_target_1(&store, &bucket, object).await;
            // The copy consumed 4 MiB of target 1's 100 MiB; a 44 MiB commit
            // leaves 4 MiB, under the object's 8 MiB peak, while target 1 stays
            // within its temporary-copy budget.
            commit_other_mutation_on_target_1(&store, 44 << 20).await;

            run_routine_to_a_deferral_pause(
                &store,
                1,
                "target pool 1 no longer fits the selected mutation: source pool 0 reservation estimate is exhausted",
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_pause_names_a_stale_selection_over_a_later_busy_gate() {
        run_large_stack_async_test("decommission-deferral-cause-priority", || async {
            let (_temp_dirs, store, bucket) = new_stale_target_store("deferral-cause-priority").await;
            let pinned = "a-pinned-large.bin";
            put_source_object(&store, &bucket, pinned, vec![0x34; 4 << 20]).await;
            // In the same set and after the pinned object, so with one entry
            // at a time its deferral settles last in the replay.
            let busy = source_key_after(&store, pinned, "busy");
            put_source_object(&store, &bucket, &busy, b"deferred on a busy gate".to_vec()).await;
            start_over_budget_routine_decommission(&store, &bucket, 0).await;
            copy_source_object_to_target_1(&store, &bucket, pinned).await;
            commit_other_mutation_on_target_1(&store, 44 << 20).await;
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let busy = busy.clone();
                    move |stage, _, object, _, success| stage == "migrate_object" && object == busy && success
                }),
                Some(Arc::new(|_, _, _| decommission_target_gate_busy_error(2))),
            );

            run_routine_to_a_deferral_pause(
                &store,
                1,
                "target pool 1 no longer fits the selected mutation: source pool 0 reservation estimate is exhausted",
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_completes_on_other_pools_while_one_pool_exceeds_its_temporary_budget() {
        run_large_stack_async_test("decommission-over-budget-target-pool", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("over-budget-target-pool");
            let object = "over-budget-target-pool.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the over-budget bucket");
            let mut source = PutObjReader::from_vec(b"migrates around the over-budget pool".to_vec());
            store.pools[0]
                .put_object(&bucket, object, &mut source, &ObjectOptions::default())
                .await
                .expect("write the source object before decommission starts");
            // Of the 128 MiB peak, target 1 takes 100 MiB and target 2 the
            // other 28 MiB; both stay far above the fixture and the
            // `.rustfs.sys` metadata the routine also migrates.
            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            set_decommission_capacity_info_overrides_for_test(
                store.id,
                vec![vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
                ]],
            );
            store
                .save_current_pool_meta_for_decommission_start(
                    &[0],
                    vec![crate::core::pools::DecomBucketInfo {
                        name: bucket.clone(),
                        prefix: String::new(),
                    }],
                )
                .await
                .expect("activate the decommission");
            // Target 1's temporary-copy budget is 50 MiB.
            strand_decommission_target_marker(&store, 1, 60 << 20).await;

            let canceler = crate::core::pools::DecommissionCanceler::new(CancellationToken::new());
            store.decommission_cancelers.write().await[0] = Some(canceler.clone());
            store
                .do_decommission_in_routine(canceler, 0, Arc::new(tokio::sync::Semaphore::new(1)))
                .await
                .expect("the decommission routine should return");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled,
                "one over-budget pool must not pause the decommission: complete={}, failed={}, canceled={}, blocked_reason={:?}",
                info.complete,
                info.failed,
                info.canceled,
                info.capacity_blocked_reason
            );
            drop(local);
            store.pools[2]
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("the object must migrate to the pool within its budget");
            assert!(
                store.pools[1]
                    .get_object_info(&bucket, object, &ObjectOptions::default())
                    .await
                    .is_err(),
                "target selection must skip the over-budget pool"
            );
        });
    }

    /// A transitioned version moves to a target pool as tier metadata, so the
    /// target copy becomes the live owner of the remote object. Removing the
    /// version from the source must not append a tier free version there: it
    /// would fail the post-check and claim a remote object the target still
    /// reads (rustfs/backlog#2873, defect D).
    #[test]
    #[serial_test::serial]
    fn decommission_of_a_transitioned_object_completes_without_a_source_free_version() {
        run_large_stack_async_test("decommission-transitioned-source-cleanup", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("transitioned-cleanup");
            let object = "transitioned.bin";
            let payload = b"transitioned source bytes".to_vec();
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the transitioned bucket");
            let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
            let source = store.pools[0]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(payload.clone()),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the transitioned source");
            store.pools[0]
                .transition_object(
                    &bucket,
                    object,
                    &ObjectOptions {
                        versioned: true,
                        version_id: source.version_id.map(|version| version.to_string()),
                        transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.clone(),
                            etag: source.etag.clone().expect("the transitioned source should have an ETag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("transition the source before the decommission");
            let source_versions = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("the source versions should decode")
                .expect("the transitioned source should exist");
            assert_eq!(source_versions.versions.len(), 1);
            let source_version = source_versions.versions[0].clone();
            assert_eq!(source_version.transition_status, rustfs_filemeta::TRANSITION_COMPLETE);
            let remote_object = source_version.transitioned_objname.clone();
            assert!(backend.contains(&remote_object).await, "the transition must upload the remote object");

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            start_routine_decommission_with_capacity(
                &store,
                &bucket,
                vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
                ],
                0,
            )
            .await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission of a transitioned object should pass its post-check");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled,
                "complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert_eq!(info.items_decommission_failed, 0);
            drop(local);

            let left = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("the source versions should decode");
            assert!(
                left.as_ref().is_none_or(|versions| versions.versions.is_empty()),
                "the source must keep neither the version nor a tier free version: {:?}",
                left.map(|versions| versions.versions)
            );

            let mut owners = Vec::new();
            for target_pool_index in [1, 2] {
                if let Some(versions) = store.pools[target_pool_index]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(&bucket, object)
                    .await
                    .expect("the target versions should decode")
                    .filter(|versions| !versions.versions.is_empty())
                {
                    owners.push((target_pool_index, versions));
                }
            }
            assert_eq!(owners.len(), 1, "exactly one target pool must own the migrated version");
            let (target_pool_index, target_versions) = owners.pop().expect("one owner");
            assert_eq!(
                target_versions.versions.len(),
                1,
                "the target must hold the version and no tier free version: {:?}",
                target_versions.versions
            );
            let moved = &target_versions.versions[0];
            assert!(!moved.tier_free_version());
            assert_eq!(moved.version_id, source_version.version_id);
            assert_eq!(moved.transition_status, rustfs_filemeta::TRANSITION_COMPLETE);
            assert_eq!(moved.transition_tier, tier_name);
            assert_eq!(moved.transitioned_objname, remote_object);
            assert_eq!(moved.transition_version_id, source_version.transition_version_id);

            assert!(backend.contains(&remote_object).await, "the target still reads the remote object");
            assert_eq!(backend.remove_count().await, 0, "no remote delete may be issued");
            assert_eq!(backend.exact_remove_count(), 0, "no remote delete may be issued");
            let mut reader = store.pools[target_pool_index]
                .get_object_reader(
                    &bucket,
                    object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        versioned: true,
                        version_id: source_version.version_id.map(|version| version.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("the migrated version should be readable from its tier");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("drain the migrated version");
            assert_eq!(body, payload);
        });
    }

    async fn exact_versions(
        store: &Arc<crate::store::ECStore>,
        pool_index: usize,
        bucket: &str,
        object: &str,
    ) -> Vec<rustfs_filemeta::FileInfo> {
        store.pools[pool_index]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("the pool versions should decode")
            .map(|versions| versions.versions)
            .unwrap_or_default()
    }

    /// Write a version to pool 0 and transition it to `tier_name`.
    async fn put_transitioned_source_version(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        tier_name: &str,
        versioned: bool,
        mod_time: Option<time::OffsetDateTime>,
    ) -> crate::object_api::ObjectInfo {
        let written = store.pools[0]
            .put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(format!("source bytes of {object}").into_bytes()),
                &ObjectOptions {
                    versioned,
                    mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("write the transitioned source");
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    versioned,
                    version_id: written.version_id.map(|version| version.to_string()),
                    transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: written.etag.clone().expect("the transitioned source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: written.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("transition the source before the decommission");
        written
    }

    /// Restore a transitioned source version, so that it keeps a local copy
    /// next to its remote object and moves as data rather than tier metadata.
    async fn restore_source_version(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        versioned: bool,
        written: &crate::object_api::ObjectInfo,
    ) {
        let mut restore_opts = ObjectOptions {
            versioned,
            version_id: written.version_id.map(|version| version.to_string()),
            ..Default::default()
        };
        restore_opts.transition.restore_request.days = Some(1);
        Arc::clone(store)
            .restore_transitioned_object(bucket, object, &restore_opts)
            .await
            .expect("restore the transitioned source");
    }

    async fn start_transitioned_entry_decommission(store: &Arc<crate::store::ECStore>, bucket: &str) {
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        start_routine_decommission_with_capacity(
            store,
            bucket,
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
            ],
            0,
        )
        .await;
    }

    fn assert_same_remote_tuple(actual: &rustfs_filemeta::FileInfo, expected: &rustfs_filemeta::FileInfo) {
        assert_eq!(actual.transition_tier, expected.transition_tier);
        assert_eq!(actual.transitioned_objname, expected.transitioned_objname);
        assert_eq!(actual.transition_version_id, expected.transition_version_id);
        assert_eq!(
            crate::services::tier::tier::tier_destination_id_from_metadata(&actual.metadata)
                .expect("the copy's destination identity should decode"),
            crate::services::tier::tier::tier_destination_id_from_metadata(&expected.metadata)
                .expect("the source destination identity should decode"),
        );
    }

    /// A delete marker copy that keeps losing target drive quorum through its
    /// in-place retries is deferred and copied again at the end of the round
    /// (rustfs/backlog#2873, defect F). It is not a permanent per-object
    /// failure, and the source is removed only after every version committed.
    #[test]
    #[serial_test::serial]
    fn decommission_defers_a_delete_marker_copy_that_lost_target_quorum() {
        run_large_stack_async_test("decommission-marker-quorum-deferral", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("marker-quorum");
            let object = "versioned/03.bin";
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create the versioned bucket");
            let mut written = Vec::new();
            for index in 0..2u8 {
                written.push(
                    store.pools[0]
                        .put_object(
                            &bucket,
                            object,
                            &mut PutObjReader::from_vec(vec![index; 2048]),
                            &ObjectOptions {
                                versioned: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("write a source version"),
                );
            }
            let marker = store.pools[0]
                .delete_object(
                    &bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the source delete marker");
            assert!(marker.delete_marker);

            // Every in-place attempt of the first pass loses quorum; the
            // deferred replay commits.
            let failing_calls = crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS;
            let marker_calls = Arc::new(AtomicUsize::new(0));
            let source_present_at_marker_commit = Arc::new(StdMutex::new(Vec::new()));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let marker_calls = Arc::clone(&marker_calls);
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "delete_marker_copy"
                            && entry_bucket == bucket
                            && entry_object == object
                            && marker_calls.fetch_add(1, Ordering::AcqRel) < failing_calls
                    }
                }),
                Some(Arc::new(|_, bucket, object| {
                    crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned())
                })),
            );
            let cleanup_observer = {
                let store = Arc::clone(&store);
                let bucket = bucket.clone();
                let seen = Arc::clone(&source_present_at_marker_commit);
                crate::core::pools::DecommissionCleanupMutationGuard::install(Arc::new(move |hook_bucket, hook_object, _| {
                    let store = Arc::clone(&store);
                    let bucket = bucket.clone();
                    let seen = Arc::clone(&seen);
                    let matches = hook_bucket == bucket && hook_object == object;
                    Box::pin(async move {
                        if matches {
                            // Cleanup only starts once the target holds every version.
                            let on_targets = exact_versions(&store, 1, &bucket, object).await.len()
                                + exact_versions(&store, 2, &bucket, object).await.len();
                            seen.lock().unwrap().push(on_targets);
                        }
                    })
                }))
            };

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission routine should return");
            drop(cleanup_observer);

            assert_eq!(
                marker_calls.load(Ordering::Acquire),
                failing_calls + 1,
                "the first pass exhausts its in-place retries and the replay copies the marker once"
            );
            assert_eq!(
                *source_present_at_marker_commit.lock().unwrap(),
                vec![3],
                "the source is cleaned up once, after the target committed all three versions"
            );
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert_eq!(info.items_decommission_failed, 0, "a lost quorum is not a permanent failure: {info:?}");
            assert!(
                info.complete && !info.failed && !info.canceled && info.capacity_blocked_reason.is_none(),
                "the replayed entry must complete the decommission: {info:?}"
            );
            drop(local);
            assert!(exact_versions(&store, 0, &bucket, object).await.is_empty(), "the source is cleaned up");
            for version in written.iter().chain(std::iter::once(&marker)) {
                let found = store
                    .get_object_info(
                        &bucket,
                        object,
                        &ObjectOptions {
                            versioned: true,
                            version_id: version.version_id.map(|id| id.to_string()),
                            ..Default::default()
                        },
                    )
                    .await;
                if version.delete_marker {
                    assert!(found.is_err(), "the marker version stays a delete marker");
                } else {
                    assert_eq!(found.expect("every data version stays readable").version_id, version.version_id);
                }
            }
        });
    }

    /// A partially migrated entry leaves some versions on the target while the
    /// retained source still holds all of them, so the two pools disagree on
    /// the version count. A copied version must stay readable from either
    /// pool instead of failing closed as an identity conflict, and a target
    /// write that keeps losing quorum pauses the decommission instead of
    /// counting a permanent failure (rustfs/backlog#2873, defect F).
    #[test]
    #[serial_test::serial]
    fn decommission_partial_entry_keeps_every_version_readable() {
        run_large_stack_async_test("decommission-partial-entry-readable", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("partial-entry");
            let object = "versioned/03.bin";
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create the versioned bucket");
            let mut written = Vec::new();
            for index in 0..3u8 {
                written.push(
                    store.pools[0]
                        .put_object(
                            &bucket,
                            object,
                            &mut PutObjReader::from_vec(vec![index; 2048]),
                            &ObjectOptions {
                                versioned: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("write a source version"),
                );
            }
            store.pools[0]
                .delete_object(
                    &bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the source delete marker");

            // The newest data version migrates; every later target write of
            // this entry loses quorum before it commits, in place and in the
            // deferred replay.
            let writes = Arc::new(AtomicUsize::new(0));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let writes = Arc::clone(&writes);
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "before_target_commit"
                            && entry_bucket == bucket
                            && entry_object == object
                            && writes.fetch_add(1, Ordering::AcqRel) >= 1
                    }
                }),
                Some(Arc::new(|_, bucket, object| {
                    crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned())
                })),
            );

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("a paused decommission routine returns without an error");

            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert_eq!(info.items_decommission_failed, 0, "a lost quorum is not a permanent failure: {info:?}");
            assert!(
                !info.complete && !info.failed && !info.canceled,
                "the entry must pause the decommission: {info:?}"
            );
            let reason = info.capacity_blocked_reason.as_deref().unwrap_or_default();
            assert!(reason.contains("lost drive quorum"), "{reason}");
            drop(local);

            assert_eq!(exact_versions(&store, 0, &bucket, object).await.len(), 4, "the source is retained");
            let on_target =
                exact_versions(&store, 1, &bucket, object).await.len() + exact_versions(&store, 2, &bucket, object).await.len();
            assert_eq!(on_target, 2, "the marker and the newest version reached the target");

            for version in &written {
                let opts = ObjectOptions {
                    versioned: true,
                    version_id: version.version_id.map(|id| id.to_string()),
                    ..Default::default()
                };
                let mut reader = store
                    .get_object_reader(&bucket, object, None, HeaderMap::new(), &opts)
                    .await
                    .unwrap_or_else(|err| panic!("version {:?} must stay readable: {err:?}", version.version_id));
                let mut body = Vec::new();
                reader.stream.read_to_end(&mut body).await.expect("read the version body");
                assert_eq!(body.len(), 2048);
                assert_eq!(reader.object_info.version_id, version.version_id);
            }
            // The latest version is the copied delete marker: reads answer
            // not found, not a cross-pool identity conflict.
            let latest = store
                .get_object_reader(
                    &bucket,
                    object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .err()
                .expect("the latest version is a delete marker");
            assert!(
                crate::error::is_err_object_not_found(&latest) || crate::error::is_err_version_not_found(&latest),
                "{latest:?}"
            );
        });
    }

    struct FirstPayloadReadQuorum {
        reads: Arc<AtomicUsize>,
        disk_error: bool,
    }

    impl tokio::io::AsyncRead for FirstPayloadReadQuorum {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if buf.remaining() == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            self.reads.fetch_add(1, Ordering::AcqRel);
            // Both producer paths fail before filling any bytes of the first stripe.
            let err = if self.disk_error {
                std::io::Error::other(crate::disk::error::DiskError::ErasureReadQuorum)
            } else {
                std::io::Error::other(crate::error::Error::ErasureReadQuorum)
            };
            std::task::Poll::Ready(Err(err))
        }
    }

    async fn assert_source_stream_quorum_is_not_a_target_failure(multipart: bool) {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("source-stream-quorum");
        let object = "source.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the bucket");
        if multipart {
            put_source_multipart_object(&store, &bucket, object).await;
        } else {
            put_source_object(&store, &bucket, object, vec![0x5a; 2048]).await;
        }
        start_transitioned_entry_decommission(&store, &bucket).await;
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the source bucket incarnation");
        let mut source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the real source must open successfully before its payload fails");
        assert_eq!(source_reader.object_info.is_multipart(), multipart);
        let reads = Arc::new(AtomicUsize::new(0));
        source_reader.stream = Box::new(FirstPayloadReadQuorum {
            reads: Arc::clone(&reads),
            disk_error: multipart,
        });
        let source_rechecks = Arc::new(AtomicUsize::new(0));
        let _observer = DecommissionTestFaultGuard::install(Arc::new({
            let bucket = bucket.clone();
            let source_rechecks = Arc::clone(&source_rechecks);
            move |stage, entry_bucket, entry_object, _, succeeded| {
                if stage == "source_recheck" && entry_bucket == bucket && entry_object == object && succeeded {
                    source_rechecks.fetch_add(1, Ordering::AcqRel);
                }
                false
            }
        }));
        let err = data_movement::migrate_decommission_object(
            Arc::clone(&store),
            0,
            bucket.clone(),
            source_reader,
            Some(incarnation),
            "decommission_source_payload_test",
            Some(owner),
        )
        .await
        .expect_err("the actual upload must return the source payload failure");

        assert!(
            source_rechecks.load(Ordering::Acquire) > 0,
            "source metadata must pass the copy-time recheck"
        );
        assert!(
            reads.load(Ordering::Acquire) > 0,
            "the actual upload must poll the failing source payload"
        );
        assert_eq!(
            exact_versions(&store, 0, &bucket, object).await.len(),
            1,
            "the source must remain present"
        );
        assert!(exact_versions(&store, 1, &bucket, object).await.is_empty());
        assert!(exact_versions(&store, 2, &bucket, object).await.is_empty());
        assert!(
            !crate::core::pools::is_decommission_copy_quorum_error_for_test(&err),
            "a source payload failure must not defer the entry as a target failure: {err:?}"
        );
        if multipart {
            assert!(
                err.to_string().contains("read_part failed"),
                "the actual upload must classify the source read: {err}"
            );
        } else {
            assert!(
                data_movement::is_data_movement_source_read_error(&err),
                "the put stage must preserve source origin: {err}"
            );
        }
    }

    #[test]
    #[serial_test::serial]
    fn decommission_does_not_defer_a_singlepart_source_stream_quorum_failure() {
        run_large_stack_async_test("decommission-singlepart-source-stream-quorum", || async {
            assert_source_stream_quorum_is_not_a_target_failure(false).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_does_not_defer_a_multipart_source_stream_quorum_failure() {
        run_large_stack_async_test("decommission-multipart-source-stream-quorum", || async {
            assert_source_stream_quorum_is_not_a_target_failure(true).await;
        });
    }

    /// Multipart upload errors carry a part-stage context. That real wrapper
    /// must keep a target quorum failure retryable by the full routine.
    #[test]
    #[serial_test::serial]
    fn decommission_defers_a_multipart_part_copy_that_lost_target_quorum() {
        run_large_stack_async_test("decommission-multipart-part-quorum", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("multipart-part-quorum");
            let object = "multipart.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the bucket");
            put_source_object(&store, &bucket, object, vec![0x5a; 2048]).await;
            let writes = Arc::new(AtomicUsize::new(0));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "before_target_commit" && entry_bucket == bucket && entry_object == object
                    }
                }),
                Some(Arc::new({
                    let writes = Arc::clone(&writes);
                    move |_, bucket, object| {
                        writes.fetch_add(1, Ordering::AcqRel);
                        let err = data_movement::data_movement_part_stage_error_for_test(
                            "decommission_object",
                            "put_object_part",
                            bucket,
                            object,
                            1,
                            crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned()),
                        );
                        assert!(
                            err.to_string()
                                .contains(&format!("put_object_part failed for {bucket}/{object} part 1")),
                            "inject the multipart part wrapper used by the copy path: {err}"
                        );
                        err
                    }
                })),
            );

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("a paused decommission routine returns without an error");

            assert!(writes.load(Ordering::Acquire) > 1, "the copy must exhaust its in-place retries");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert_eq!(
                info.items_decommission_failed, 0,
                "a lost target quorum is not a permanent failure: {info:?}"
            );
            assert!(
                !info.complete && !info.failed && !info.canceled,
                "the entry must pause the decommission: {info:?}"
            );
            let reason = info.capacity_blocked_reason.as_deref().unwrap_or_default();
            assert!(reason.contains("lost drive quorum"), "{reason}");
            drop(local);

            assert_eq!(exact_versions(&store, 0, &bucket, object).await.len(), 1, "the source is retained");
            assert!(exact_versions(&store, 1, &bucket, object).await.is_empty());
            assert!(exact_versions(&store, 2, &bucket, object).await.is_empty());
        });
    }

    /// A node killed mid-copy leaves its namespace lock on the peers until the
    /// lock lease expires, so the resumed decommission times out acquiring the
    /// lock of the key it was copying through every in-place retry. The entry
    /// is deferred and copied once the lease is gone; it is never counted as a
    /// failed object, which would fail the whole decommission.
    #[test]
    #[serial_test::serial]
    fn decommission_defers_a_copy_that_timed_out_on_a_stale_namespace_lock() {
        run_large_stack_async_test("decommission-copy-lock-timeout", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("copy-lock-timeout");
            let object = "external/005.bin";
            let payload = vec![0x3c; 2048];
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the bucket");
            put_source_object(&store, &bucket, object, payload.clone()).await;

            // The stale lease outlives every in-place attempt of the first
            // pass and has expired by the deferred replay.
            let stale_lease_calls = crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS;
            let copy_calls = Arc::new(AtomicUsize::new(0));
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let copy_calls = Arc::clone(&copy_calls);
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "before_target_commit"
                            && entry_bucket == bucket
                            && entry_object == object
                            && copy_calls.fetch_add(1, Ordering::AcqRel) < stale_lease_calls
                    }
                }),
                Some(Arc::new(|_, bucket, object| {
                    // The shape the target write reports when the lock domain
                    // still holds the previous incarnation's lock.
                    data_movement::data_movement_stage_error_for_test(
                        "decommission_object",
                        "prepare_put_object",
                        bucket,
                        object,
                        crate::error::Error::Lock(rustfs_lock::LockError::timeout(
                            format!("{bucket}/{object}@latest"),
                            Duration::from_secs(5),
                        )),
                    )
                })),
            );

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission routine should return");

            assert_eq!(
                copy_calls.load(Ordering::Acquire),
                stale_lease_calls + 1,
                "the first pass exhausts its in-place retries and the replay copies the object once"
            );
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert_eq!(
                info.items_decommission_failed, 0,
                "a lock held until its lease expires is not a permanent failure: {info:?}"
            );
            assert!(
                info.complete && !info.failed && !info.canceled && info.capacity_blocked_reason.is_none(),
                "the replayed entry must complete the decommission: {info:?}"
            );
            drop(local);

            assert!(exact_versions(&store, 0, &bucket, object).await.is_empty(), "the source is cleaned up");
            let mut reader = store
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the migrated object stays readable");
            let mut body = Vec::new();
            reader.stream.read_to_end(&mut body).await.expect("read the migrated object");
            assert_eq!(body, payload);
        });
    }

    /// Seed `object` in pool 0 with three 2 KiB data versions and a newest
    /// delete marker; returns the data versions oldest first.
    async fn seed_versioned_entry_with_marker(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
    ) -> Vec<crate::object_api::ObjectInfo> {
        store
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create the versioned bucket");
        let mut written = Vec::new();
        for index in 0..3u8 {
            written.push(
                store.pools[0]
                    .put_object(
                        bucket,
                        object,
                        &mut PutObjReader::from_vec(vec![index; 2048]),
                        &ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("write a source version"),
            );
        }
        store.pools[0]
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write the source delete marker");
        written
    }

    /// Fail every target write of `object` after the first, before it
    /// commits, while `armed` is set.
    fn fail_target_writes_after_the_first(
        bucket: &str,
        object: &'static str,
        armed: Arc<AtomicBool>,
    ) -> DecommissionTestFaultGuard {
        let writes = Arc::new(AtomicUsize::new(0));
        let bucket = bucket.to_owned();
        DecommissionTestFaultGuard::install_with_error(
            Arc::new(move |stage, entry_bucket, entry_object, _, _| {
                stage == "before_target_commit"
                    && entry_bucket == bucket
                    && entry_object == object
                    && armed.load(Ordering::Acquire)
                    && writes.fetch_add(1, Ordering::AcqRel) >= 1
            }),
            Some(Arc::new(|_, bucket, object| {
                crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned())
            })),
        )
    }

    async fn assert_every_version_readable(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        written: &[crate::object_api::ObjectInfo],
    ) {
        for (index, version) in written.iter().enumerate() {
            let opts = ObjectOptions {
                versioned: true,
                version_id: version.version_id.map(|id| id.to_string()),
                ..Default::default()
            };
            let mut reader = store
                .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
                .await
                .unwrap_or_else(|err| panic!("version {:?} must stay readable: {err:?}", version.version_id));
            let mut body = Vec::new();
            reader.stream.read_to_end(&mut body).await.expect("read the version body");
            assert_eq!(body, vec![index as u8; 2048], "version {:?}", version.version_id);
        }
    }

    fn decommission_info(pool_meta: &crate::core::pools::PoolMeta) -> crate::core::pools::PoolDecommissionInfo {
        pool_meta.pools[0]
            .decommission
            .clone()
            .expect("the decommission state should remain present")
    }

    /// Replace the in-memory pool metadata with what the replicas persisted,
    /// as a restarted leader would load it.
    async fn reload_persisted_pool_meta(store: &Arc<crate::store::ECStore>) {
        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(store.pools.clone())
            .await
            .expect("reload the persisted pool metadata");
        *store.pool_meta.write().await = persisted;
    }

    /// Wait until the relaunched routine recorded its `repeats`-th pause and
    /// released its worker slot.
    async fn wait_for_relaunched_pause(store: &Arc<crate::store::ECStore>, repeats: u32) {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let paused = store
                    .ctx
                    .decommission_pause(0)
                    .is_some_and(|record| record.repeats >= repeats);
                if paused && !store.has_active_local_decommission_worker().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the relaunched routine should pause again: record={:?}", store.ctx.decommission_pause(0)));
    }

    /// A partially moved entry pauses the decommission, and the resumed
    /// routine, started from the persisted metadata, finds its first versions
    /// already on the target. Every version is counted exactly once
    /// (rustfs/backlog#2873, defect F follow-up).
    #[test]
    #[serial_test::serial]
    fn decommission_counts_each_version_once_across_a_pause_and_resume() {
        run_large_stack_async_test("decommission-pause-resume-count", || async {
            // Serialize this accounting fixture so the deferred entry precedes
            // the later checkpoint, regardless of worker scheduling.
            let env = [
                ("RUSTFS_DECOMMISSION_BUCKET_CONCURRENCY", Some("1")),
                ("RUSTFS_DECOMMISSION_ENTRY_CONCURRENCY", Some("1")),
            ];
            temp_env::async_with_vars(env, async {
                let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
                let bucket = test_bucket("pause-resume-count");
                let object = "versioned/03.bin";
                let written = seed_versioned_entry_with_marker(&store, &bucket, object).await;
                // Listed after the partially moved entry: its completion saves
                // a progress checkpoint while that entry's first versions are
                // moved and its later ones are still deferred.
                let source_set = store.pools[0].get_disks_by_key(object).set_index;
                let later = (0..1024)
                    .map(|index| format!("z/after-{index:04}.bin"))
                    .find(|key| store.pools[0].get_disks_by_key(key).set_index == source_set)
                    .expect("a later key in the same source set");
                store.pools[0]
                    .put_object(
                        &bucket,
                        &later,
                        &mut PutObjReader::from_vec(vec![0x7a; 16]),
                        &ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("write the later entry");

                // Counters when the test bucket's first entry starts: only the
                // system objects are in them.
                let baseline = Arc::new(StdMutex::new(None::<(String, usize, usize)>));
                let armed = Arc::new(AtomicBool::new(true));
                let writes = Arc::new(AtomicUsize::new(0));
                let _fault = DecommissionTestFaultGuard::install_with_error(
                    Arc::new({
                        let store = Arc::clone(&store);
                        let bucket = bucket.clone();
                        let baseline = Arc::clone(&baseline);
                        let armed = Arc::clone(&armed);
                        move |stage, entry_bucket, entry_object, _, _| {
                            if entry_bucket != bucket {
                                return false;
                            }
                            if stage == "exact_versions_read" {
                                let mut baseline = baseline.lock().expect("baseline lock");
                                if baseline.is_none() {
                                    // This synchronous fault hook must not miss
                                    // the baseline when metadata is briefly busy.
                                    let pool_meta = tokio::task::block_in_place(|| store.pool_meta.blocking_read());
                                    let info = pool_meta.pools[0].decommission.as_ref().expect("active decommission");
                                    *baseline = Some((entry_object.to_owned(), info.items_decommissioned, info.bytes_done));
                                }
                                return false;
                            }
                            stage == "before_target_commit"
                                && entry_object == object
                                && armed.load(Ordering::Acquire)
                                && writes.fetch_add(1, Ordering::AcqRel) >= 1
                        }
                    }),
                    Some(Arc::new(|_, bucket, object| {
                        crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned())
                    })),
                );
                let _checkpoint = crate::core::pools::DecommissionCleanupMutationGuard::install(Arc::new({
                    let store = Arc::clone(&store);
                    let bucket = bucket.clone();
                    move |hook_bucket, hook_object, _| {
                        let store = Arc::clone(&store);
                        let due = hook_bucket == bucket && hook_object == later;
                        Box::pin(async move {
                            if due {
                                let stale = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
                                let mut pool_meta = store.pool_meta.write().await;
                                pool_meta.pools[0].last_update = stale;
                                if let Some(info) = pool_meta.pools[0].decommission.as_mut() {
                                    info.progress_save_last_at = Some(stale);
                                }
                            }
                        })
                    }
                }));

                start_transitioned_entry_decommission(&store, &bucket).await;
                spawn_decommission_routine(&store, 1)
                    .await
                    .expect("the decommission routine task should join")
                    .expect("a paused decommission routine returns without an error");
                let paused = decommission_info(&*store.pool_meta.read().await);
                assert!(!paused.complete && !paused.failed, "{paused:?}");
                assert!(paused.capacity_blocked_reason.is_some(), "{paused:?}");
                assert_eq!(paused.items_decommission_failed, 0, "{paused:?}");
                let (first_entry, base_items, base_bytes) = baseline
                    .lock()
                    .expect("baseline lock")
                    .clone()
                    .expect("the test bucket's first entry started");
                assert_eq!(first_entry, object, "the baseline precedes every entry of the test bucket");

                reload_persisted_pool_meta(&store).await;
                armed.store(false, Ordering::Release);
                spawn_decommission_routine(&store, 1)
                    .await
                    .expect("the resumed routine task should join")
                    .expect("the resumed routine should return");
                let done = decommission_info(&*store.pool_meta.read().await);
                assert!(done.complete && !done.failed && done.capacity_blocked_reason.is_none(), "{done:?}");
                assert_eq!(
                    (done.items_decommissioned, done.bytes_done),
                    (base_items + 5, base_bytes + 3 * 2048 + 16),
                    "the entry's four versions and the later entry, each counted once: {done:?}"
                );
                assert_eq!(done.items_decommission_failed, 0, "{done:?}");
                assert!(exact_versions(&store, 0, &bucket, object).await.is_empty(), "the source is cleaned up");
                assert_every_version_readable(&store, &bucket, object, &written).await;
            })
            .await;
        });
    }

    /// Leave `object`'s target metadata unusable on two of the target set's
    /// four drives, below the read and write quorum of EC 3+1: a directory
    /// where `xl.meta` belongs fails every read and write there.
    async fn break_target_metadata_on_two_drives(
        temp_dirs: &[tempfile::TempDir],
        store: &Arc<crate::store::ECStore>,
        target: usize,
        bucket: &str,
        object: &str,
    ) {
        let set_index = store.pools[target].get_disks_by_key(object).set_index;
        for disk in 0..2 {
            let meta = temp_dirs[target * 8 + set_index * 4 + disk]
                .path()
                .join(bucket)
                .join(object)
                .join("xl.meta");
            tokio::fs::remove_file(&meta)
                .await
                .unwrap_or_else(|err| panic!("remove {}: {err}", meta.display()));
            tokio::fs::create_dir(&meta)
                .await
                .unwrap_or_else(|err| panic!("block {}: {err}", meta.display()));
        }
    }

    /// A target that holds the entry's metadata on fewer drives than its
    /// quorum is unreadable. The intact source keeps serving every version,
    /// and a pass that meets the sub-quorum target defers the entry: it pauses
    /// without a failed item and counts nothing of the entry.
    #[test]
    #[serial_test::serial]
    fn decommission_half_written_target_never_shadows_the_source() {
        run_large_stack_async_test("decommission-half-written-target", || async {
            let (temp_dirs, store, _other_store) =
                crate::services::rebalance::test_three_pool_stores_with_four_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("half-written-target");
            let object = "versioned/03.bin";
            let written = seed_versioned_entry_with_marker(&store, &bucket, object).await;
            let armed = Arc::new(AtomicBool::new(true));
            let _fault = fail_target_writes_after_the_first(&bucket, object, Arc::clone(&armed));

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("a paused decommission routine returns without an error");
            let mut target = None;
            for pool in [1, 2] {
                if !exact_versions(&store, pool, &bucket, object).await.is_empty() {
                    target = Some(pool);
                }
            }
            let target = target.expect("the marker and the newest version reached a target");
            break_target_metadata_on_two_drives(&temp_dirs, &store, target, &bucket, object).await;
            let moved = store.pools[target]
                .get_object_info(
                    &bucket,
                    object,
                    &ObjectOptions {
                        versioned: true,
                        version_id: written[2].version_id.map(|id| id.to_string()),
                        ..Default::default()
                    },
                )
                .await;
            assert!(moved.is_err(), "the target copy must be unreadable: {moved:?}");
            assert_every_version_readable(&store, &bucket, object, &written).await;

            // No injected fault any more: only the sub-quorum target remains.
            armed.store(false, Ordering::Release);
            let before = decommission_info(&*store.pool_meta.read().await);
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the resumed routine task should join")
                .expect("a paused decommission routine returns without an error");
            let info = decommission_info(&*store.pool_meta.read().await);
            assert!(!info.complete && !info.failed, "the entry is deferred, not settled: {info:?}");
            let reason = info.capacity_blocked_reason.as_deref().unwrap_or_default();
            assert!(reason.contains("lost drive quorum"), "{reason}");
            assert_eq!(info.items_decommission_failed, 0, "{info:?}");
            assert_eq!(
                (info.items_decommissioned, info.bytes_done),
                (before.items_decommissioned, before.bytes_done),
                "a deferred entry counts nothing: {info:?}"
            );
            assert_eq!(exact_versions(&store, 0, &bucket, object).await.len(), 4, "the source is retained");
            assert_every_version_readable(&store, &bucket, object, &written).await;
        });
    }

    /// A bucket holding a permanently retained entry and an entry that keeps
    /// losing target quorum pauses on the same cause every pass. The retained
    /// entry is not counted again, so the watchdog's relaunch backs off; a
    /// first pause is relaunched by the watchdog path and a repeated one is
    /// held (rustfs/backlog#2873, defect F follow-up).
    #[test]
    #[serial_test::serial]
    fn decommission_relaunch_backs_off_while_a_retained_entry_is_relisted() {
        assert_retained_entry_relaunch_backoff(false);
    }

    #[test]
    #[serial_test::serial]
    fn decommission_full_count_ledger_preserves_failure_and_relaunch_backoff() {
        assert_retained_entry_relaunch_backoff(true);
    }

    fn assert_retained_entry_relaunch_backoff(full_ledger: bool) {
        run_large_stack_async_test("decommission-retained-backoff", move || async move {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("retained-backoff");
            let object = "versioned/03.bin";
            let retained = "a/retained.bin";
            seed_versioned_entry_with_marker(&store, &bucket, object).await;
            store.pools[0]
                .put_object(
                    &bucket,
                    retained,
                    &mut PutObjReader::from_vec(vec![0x52; 64]),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the retained entry");
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        entry_bucket == bucket
                            && ((stage == "object_read" && entry_object == retained)
                                || (stage == "before_target_commit" && entry_object == object))
                    }
                }),
                Some(Arc::new(|stage, bucket, object| {
                    if stage == "object_read" {
                        crate::error::Error::other("injected permanent source read failure")
                    } else {
                        crate::error::Error::InsufficientWriteQuorum(bucket.to_owned(), object.to_owned())
                    }
                })),
            );

            start_transitioned_entry_decommission(&store, &bucket).await;
            if full_ledger {
                let generation = decommission_info(&*store.pool_meta.read().await)
                    .start_time
                    .expect("generation");
                store.ctx.with_decommission_count_ledger(0, generation, |ledger| {
                    for index in 0..2048 {
                        ledger.admit("retained-ledger-fixture", &format!("key-{index}"), (None, false), false);
                    }
                    assert!(!ledger.admit("retained-ledger-fixture", "overflow", (None, false), false));
                });
            }
            let pause_round = |round: u32| {
                let store = Arc::clone(&store);
                async move {
                    let info = decommission_info(&*store.pool_meta.read().await);
                    assert!(!info.complete && !info.failed, "round {round} pauses: {info:?}");
                    assert!(info.capacity_blocked_reason.is_some(), "round {round}: {info:?}");
                    assert_eq!(
                        info.items_decommission_failed, 1,
                        "the retained version is counted once (round {round}): {info:?}"
                    );
                    let record = store.ctx.decommission_pause(0).expect("the pause is recorded");
                    assert_eq!(record.repeats, round, "round {round}: {record:?}");
                    info
                }
            };

            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("a paused decommission routine returns without an error");
            let first = pause_round(1).await;
            // The test helper keeps its worker slot; a finished routine frees it.
            store.decommission_cancelers.write().await[0] = None;

            // A first pause holds nothing: the watchdog path relaunches it.
            store
                .spawn_missing_local_decommission_routines()
                .await
                .expect("the watchdog relaunches a first pause");
            assert!(
                store.has_active_local_decommission_worker().await
                    || store.ctx.decommission_pause(0).is_some_and(|record| record.repeats == 2),
                "the first pause must be relaunched"
            );
            wait_for_relaunched_pause(&store, 2).await;
            let second = pause_round(2).await;
            assert_eq!(second.items_decommissioned, first.items_decommissioned, "{second:?}");

            // A repeated pause is held: the watchdog path starts no worker.
            store
                .spawn_missing_local_decommission_routines()
                .await
                .expect("a held relaunch is not an error");
            assert!(!store.has_active_local_decommission_worker().await, "a repeated pause must be held");
            assert_eq!(store.ctx.decommission_pause(0).map(|record| record.repeats), Some(2));

            spawn_decommission_routine(&store, 1)
                .await
                .expect("the third routine task should join")
                .expect("a paused decommission routine returns without an error");
            let third = pause_round(3).await;
            assert_eq!(third.items_decommissioned, first.items_decommissioned, "{third:?}");
            assert_eq!(exact_versions(&store, 0, &bucket, retained).await.len(), 1, "the retained source stays");
        });
    }

    /// Only a target write is deferred. A source version that cannot be read
    /// keeps its per-object failure, so an unreadable source ends the
    /// decommission as failed instead of pausing and resuming forever.
    #[test]
    #[serial_test::serial]
    fn decommission_counts_a_source_read_that_lost_quorum_as_failed() {
        run_large_stack_async_test("decommission-source-read-quorum", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("source-read-quorum");
            let object = "unreadable.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the bucket");
            put_source_object(&store, &bucket, object, vec![0x5a; 2048]).await;
            let _fault = DecommissionTestFaultGuard::install_with_error(
                Arc::new({
                    let bucket = bucket.clone();
                    move |stage, entry_bucket, entry_object, _, _| {
                        stage == "object_read" && entry_bucket == bucket && entry_object == object
                    }
                }),
                Some(Arc::new(|_, bucket, object| {
                    crate::error::Error::InsufficientReadQuorum(bucket.to_owned(), object.to_owned())
                })),
            );

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission routine should return");

            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert_eq!(info.items_decommission_failed, 1, "{info:?}");
            assert!(info.failed && info.capacity_blocked_reason.is_none(), "{info:?}");
            drop(local);
            assert_eq!(exact_versions(&store, 0, &bucket, object).await.len(), 1, "the source is retained");
        });
    }

    async fn assert_decommission_source_recheck_quorum_failure_is_permanent(delete_marker: bool) {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("source-recheck-quorum");
        let object = "source.bin";
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: delete_marker,
                    ..Default::default()
                },
            )
            .await
            .expect("create the bucket");
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut PutObjReader::from_vec(vec![0x5a; 2048]),
                &ObjectOptions {
                    versioned: delete_marker,
                    ..Default::default()
                },
            )
            .await
            .expect("write the source version");
        if delete_marker {
            let marker = store.pools[0]
                .delete_object(
                    &bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the source delete marker");
            assert!(marker.delete_marker);
        }
        let source_versions = exact_versions(&store, 0, &bucket, object).await.len();
        assert_eq!(source_versions, if delete_marker { 2 } else { 1 });
        let source_reads = Arc::new(AtomicUsize::new(0));
        let source_rechecks = Arc::new(AtomicUsize::new(0));
        let recheck_stage = if delete_marker {
            "delete_marker_source_recheck"
        } else {
            "source_recheck"
        };
        let _fault = DecommissionTestFaultGuard::install_with_error(
            Arc::new({
                let source_reads = Arc::clone(&source_reads);
                let bucket = bucket.clone();
                move |stage, entry_bucket, entry_object, _, succeeded| {
                    if entry_bucket != bucket || entry_object != object {
                        return false;
                    }
                    if stage == "object_read" && succeeded {
                        source_reads.fetch_add(1, Ordering::AcqRel);
                    }
                    stage == recheck_stage
                }
            }),
            Some(Arc::new({
                let source_rechecks = Arc::clone(&source_rechecks);
                move |_, bucket, object| {
                    source_rechecks.fetch_add(1, Ordering::AcqRel);
                    crate::error::Error::InsufficientReadQuorum(bucket.to_owned(), object.to_owned())
                }
            })),
        );

        start_transitioned_entry_decommission(&store, &bucket).await;
        spawn_decommission_routine(&store, 1)
            .await
            .expect("the decommission routine task should join")
            .expect("the decommission routine should return");

        assert!(
            source_rechecks.load(Ordering::Acquire) >= crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS,
            "the source metadata recheck inside the copy must exhaust its retries"
        );
        if !delete_marker {
            assert!(source_reads.load(Ordering::Acquire) > 0, "the initial source open must succeed");
        }
        let local = store.pool_meta.read().await;
        let info = local.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert_eq!(info.items_decommission_failed, 1, "a source failure is permanent: {info:?}");
        assert!(
            info.failed && !info.complete && !info.canceled && info.capacity_blocked_reason.is_none(),
            "a source recheck must fail the decommission instead of pausing it: {info:?}"
        );
        drop(local);
        assert_eq!(
            exact_versions(&store, 0, &bucket, object).await.len(),
            source_versions,
            "all source versions are retained"
        );
    }

    #[test]
    #[serial_test::serial]
    fn decommission_counts_a_source_recheck_that_lost_quorum_as_failed() {
        run_large_stack_async_test("decommission-source-recheck-quorum", || async {
            assert_decommission_source_recheck_quorum_failure_is_permanent(false).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_counts_a_delete_marker_source_recheck_that_lost_quorum_as_failed() {
        run_large_stack_async_test("decommission-marker-source-recheck-quorum", || async {
            assert_decommission_source_recheck_quorum_failure_is_permanent(true).await;
        });
    }

    /// A restored source moves as data; the copy keeps its transition tuple,
    /// so the target owns the remote object and the source cleanup must not
    /// leave a tier free version (rustfs/backlog#2873, defect D).
    #[test]
    #[serial_test::serial]
    fn decommission_of_a_restored_transitioned_version_moves_its_remote_owner() {
        run_large_stack_async_test("decommission-restored-transitioned-version", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("restored-transitioned");
            let object = "restored.bin";
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create the versioned bucket");
            let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
            let written = put_transitioned_source_version(&store, &bucket, object, &tier_name, true, None).await;
            restore_source_version(&store, &bucket, object, true, &written).await;
            let source_versions = exact_versions(&store, 0, &bucket, object).await;
            assert_eq!(source_versions.len(), 1);
            let source = source_versions[0].clone();
            assert_eq!(source.transition_status, rustfs_filemeta::TRANSITION_COMPLETE);
            assert!(!source.is_remote(), "a restored source keeps its local copy");

            start_transitioned_entry_decommission(&store, &bucket).await;
            spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission of a restored transitioned version should pass its post-check");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled,
                "complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert_eq!(info.items_decommission_failed, 0);
            drop(local);

            let left = exact_versions(&store, 0, &bucket, object).await;
            assert!(left.is_empty(), "the source must keep no version or tier free version: {left:?}");
            let mut owners = Vec::new();
            for target_pool_index in [1, 2] {
                owners.extend(exact_versions(&store, target_pool_index, &bucket, object).await);
            }
            assert_eq!(owners.len(), 1, "one target must hold the version and no free version: {owners:?}");
            let moved = &owners[0];
            assert!(!moved.tier_free_version());
            assert_eq!(moved.version_id, source.version_id);
            assert_eq!(moved.transition_status, rustfs_filemeta::TRANSITION_COMPLETE);
            assert_same_remote_tuple(moved, &source);
            assert!(
                backend.contains(&source.transitioned_objname).await,
                "the remote object must stay in the tier"
            );
            assert_eq!(backend.remove_count().await, 0, "no remote delete may be issued");
            assert_eq!(backend.exact_remove_count(), 0, "no remote delete may be issued");
        });
    }

    /// A newer unversioned object written to the target replaces a restored
    /// null source instead of receiving a copy of it. The source's remote
    /// object is then referenced nowhere, so the source cleanup must keep a
    /// tier free version to delete it (rustfs/backlog#2873, defect D).
    async fn run_superseded_restored_null_version_case() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("superseded-restored");
        let object = "restored.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the unversioned bucket");
        let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
        let written = put_transitioned_source_version(&store, &bucket, object, &tier_name, false, None).await;
        restore_source_version(&store, &bucket, object, false, &written).await;
        let source_versions = exact_versions(&store, 0, &bucket, object).await;
        assert_eq!(source_versions.len(), 1);
        let source = source_versions[0].clone();
        assert!(source.version_id.is_none(), "the source must be a null version");
        assert!(!source.is_remote(), "a restored source keeps its local copy");
        let newer = b"written to the target after the source".to_vec();
        store.pools[1]
            .put_object(&bucket, object, &mut PutObjReader::from_vec(newer.clone()), &ObjectOptions::default())
            .await
            .expect("write a newer null version on the target");

        start_transitioned_entry_decommission(&store, &bucket).await;
        store
            .decommission_entry_for_test_with_bucket_incarnation(
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.clone(),
                store.pools[0].get_disks_by_key(object),
            )
            .await
            .expect("the superseded entry should finish");

        let left = exact_versions(&store, 0, &bucket, object).await;
        assert_eq!(left.len(), 1, "the source must keep one tier free version: {left:?}");
        assert!(left[0].tier_free_version());
        assert_same_remote_tuple(&left[0], &source);
        let target = exact_versions(&store, 1, &bucket, object).await;
        assert_eq!(target.len(), 1, "the target keeps only its newer version: {target:?}");
        assert!(target[0].transition_status.is_empty(), "the newer version references no remote object");
        assert!(exact_versions(&store, 2, &bucket, object).await.is_empty());
        let mut reader = store.pools[1]
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the newer target version should be readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("drain the newer version");
        assert_eq!(body, newer);
        assert!(backend.contains(&source.transitioned_objname).await);
        assert_eq!(backend.remove_count().await, 0);
    }

    #[test]
    #[serial_test::serial]
    fn decommission_keeps_a_free_version_for_a_restored_null_version_superseded_on_target() {
        run_large_stack_async_test("decommission-superseded-restored-null-version", run_superseded_restored_null_version_case);
    }

    /// Every capacity marker of the decommission's target reservations is
    /// settled: no pending intent and no temporary mutation remain.
    async fn assert_decommission_capacity_settled(store: &Arc<crate::store::ECStore>) {
        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert!(
            info.capacity_blocked_reason.is_none(),
            "the decommission must not pause: {:?}",
            info.capacity_blocked_reason
        );
        let reservation = info
            .capacity_reservation
            .as_ref()
            .expect("the capacity reservation should remain present");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        for target in &reservation.targets {
            assert!(
                target.pending_mutation_id.is_none(),
                "target {} keeps a pending intent",
                target.pool_index
            );
            assert_eq!(target.pending_physical_bytes, 0);
            assert_eq!(target.inflight_physical_bytes, 0);
            assert!(
                target.temporary_mutations.is_empty(),
                "target {} keeps temporary mutations: {:?}",
                target.pool_index,
                target.temporary_mutations
            );
        }
    }

    /// A newer unversioned object written to the target replaces a multipart
    /// null source. The multipart copy settles as superseded and aborts its
    /// staged upload; that cleanup must accept the newer target as proof the
    /// staged copy holds nothing there, instead of failing closed as a
    /// capacity block that never lets the entry finish (rustfs/backlog#2873).
    async fn run_superseded_multipart_null_version_case(tiered: bool) {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("superseded-multipart");
        let object = "multipart.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the unversioned bucket");
        let written = put_source_multipart_object(&store, &bucket, object).await;
        let backend = if tiered {
            let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
            store.pools[0]
                .transition_object(
                    &bucket,
                    object,
                    &ObjectOptions {
                        transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name,
                            etag: written.etag.clone().expect("the multipart source should have an ETag"),
                            ..Default::default()
                        },
                        mod_time: written.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("transition the multipart source before the decommission");
            restore_source_version(&store, &bucket, object, false, &written).await;
            Some(backend)
        } else {
            None
        };
        let source_versions = exact_versions(&store, 0, &bucket, object).await;
        assert_eq!(source_versions.len(), 1);
        let source = source_versions[0].clone();
        assert!(source.version_id.is_none(), "the source must be a null version");
        assert_eq!(source.parts.len(), 2, "the source must move through the multipart copy");
        assert!(!source.is_remote(), "the source keeps its local copy");
        let newer = b"written to the target after the source".to_vec();
        store.pools[1]
            .put_object(&bucket, object, &mut PutObjReader::from_vec(newer.clone()), &ObjectOptions::default())
            .await
            .expect("write a newer null version on the target");

        start_transitioned_entry_decommission(&store, &bucket).await;
        store
            .decommission_entry_for_test_with_bucket_incarnation(
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.clone(),
                store.pools[0].get_disks_by_key(object),
            )
            .await
            .expect("the superseded multipart entry should finish");

        let left = exact_versions(&store, 0, &bucket, object).await;
        if let Some(backend) = backend.as_ref() {
            // The newer target references no remote object, so the source
            // cleanup keeps the tier free version that owns the remote delete.
            assert_eq!(left.len(), 1, "the source must keep one tier free version: {left:?}");
            assert!(left[0].tier_free_version());
            assert_same_remote_tuple(&left[0], &source);
            assert!(backend.contains(&source.transitioned_objname).await);
            assert_eq!(backend.remove_count().await, 0);
        } else {
            assert!(left.is_empty(), "the superseded source must be cleaned up: {left:?}");
        }
        let target = exact_versions(&store, 1, &bucket, object).await;
        assert_eq!(target.len(), 1, "the target keeps only its newer version: {target:?}");
        assert!(!target[0].tier_free_version());
        assert!(target[0].transition_status.is_empty(), "the newer version references no remote object");
        assert!(exact_versions(&store, 2, &bucket, object).await.is_empty());
        let target_set = store.pools[1].get_disks_by_key(object);
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the bucket incarnation");
        let staged_identity = format!(
            "v1:none:{}",
            source
                .mod_time
                .expect("the source has a modification time")
                .unix_timestamp_nanos()
        );
        assert!(
            target_set
                .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &staged_identity)
                .await
                .expect("scan the target for the staged upload")
                .is_empty(),
            "the staged upload must be aborted"
        );
        let mut reader = store.pools[1]
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the newer target version should be readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("drain the newer version");
        assert_eq!(body, newer);
        assert_decommission_capacity_settled(&store).await;
    }

    #[test]
    #[serial_test::serial]
    fn decommission_finishes_a_restored_multipart_null_version_superseded_on_target() {
        run_large_stack_async_test("decommission-superseded-restored-multipart", || {
            run_superseded_multipart_null_version_case(true)
        });
    }

    /// The target already holds a null object with the multipart source's
    /// exact identity but without the data-movement ownership proof. The copy
    /// cannot publish over it, and its cleanup cannot tell whether that object
    /// is the staged copy. That identity-proof failure must fail the
    /// decommission with an accurate error, not pause it as a capacity block
    /// that only a restart could leave (rustfs/backlog#2873).
    #[test]
    #[serial_test::serial]
    fn decommission_fails_instead_of_pausing_on_an_unowned_exact_multipart_target() {
        run_large_stack_async_test("decommission-unowned-exact-multipart-target", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("unowned-exact-multipart");
            let object = "multipart.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create the unversioned bucket");
            let written = put_source_multipart_object(&store, &bucket, object).await;
            let unowned = b"same identity, written without a data movement".to_vec();
            store.pools[1]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(unowned.clone()),
                    &ObjectOptions {
                        mod_time: written.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("write the unowned exact-identity object on the target");
            let target = exact_versions(&store, 1, &bucket, object).await;
            assert_eq!(target.len(), 1);
            assert!(target[0].version_id.is_none());
            assert_eq!(target[0].mod_time, written.mod_time, "the target must carry the staged identity");

            start_transitioned_entry_decommission(&store, &bucket).await;
            let _ = spawn_decommission_routine(&store, 1)
                .await
                .expect("the decommission routine task should join");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.failed && !info.complete && info.capacity_blocked_reason.is_none(),
                "an identity-proof failure must fail the decommission, not pause it as a capacity block: complete={}, failed={}, blocked_reason={:?}",
                info.complete,
                info.failed,
                info.capacity_blocked_reason
            );
            drop(local);

            assert_eq!(exact_versions(&store, 0, &bucket, object).await.len(), 1, "the source must stay in place");
            let mut reader = store.pools[1]
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the unowned target object should stay readable");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("drain the unowned target object");
            assert_eq!(body, unowned);
        });
    }

    #[test]
    #[serial_test::serial]
    fn decommission_finishes_a_multipart_null_version_superseded_on_target() {
        run_large_stack_async_test("decommission-superseded-multipart", || run_superseded_multipart_null_version_case(false));
    }

    struct ExpiredAndMovedVersions {
        _temp_dirs: Vec<tempfile::TempDir>,
        store: Arc<crate::store::ECStore>,
        bucket: String,
        object: &'static str,
        backend: crate::services::tier::test_util::MockWarmBackend,
        expired: rustfs_filemeta::FileInfo,
        moved: rustfs_filemeta::FileInfo,
    }

    /// Write two transitioned versions of one object to pool 0 of a versioned
    /// bucket whose lifecycle expires the older, noncurrent one.
    async fn put_expired_and_moved_transitioned_versions(name: &str) -> ExpiredAndMovedVersions {
        let (temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket(name);
        let object = "versions.bin";
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create the versioned bucket");
        let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
        let now = time::OffsetDateTime::now_utc();
        let expired =
            put_transitioned_source_version(&store, &bucket, object, &tier_name, true, Some(now - time::Duration::days(10)))
                .await;
        let moved =
            put_transitioned_source_version(&store, &bucket, object, &tier_name, true, Some(now - time::Duration::days(5))).await;
        let source_versions = exact_versions(&store, 0, &bucket, object).await;
        assert_eq!(source_versions.len(), 2);
        let source = |version_id| {
            source_versions
                .iter()
                .find(|version| version.version_id == version_id)
                .cloned()
                .expect("each written version should be loaded")
        };
        let (expired, moved) = (source(expired.version_id), source(moved.version_id));
        assert_ne!(expired.transitioned_objname, moved.transitioned_objname);
        let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-noncurrent</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix></Prefix>
    </Filter>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>1</NoncurrentDays>
    </NoncurrentVersionExpiration>
  </Rule>
</LifecycleConfiguration>"#;
        crate::bucket::metadata_sys::update_in(
            &store.ctx,
            &bucket,
            crate::bucket::metadata::BUCKET_LIFECYCLE_CONFIG,
            lifecycle_xml.as_bytes().to_vec(),
        )
        .await
        .expect("store the noncurrent expiration rule");
        ExpiredAndMovedVersions {
            _temp_dirs: temp_dirs,
            store,
            bucket,
            object,
            backend,
            expired,
            moved,
        }
    }

    /// The source keeps nothing, and the target pools hold the moved version
    /// and the expired version's tier free version, its only remote owner.
    /// Nothing deleted a remote object yet.
    async fn assert_expired_version_owner_moved(case: &ExpiredAndMovedVersions) -> rustfs_filemeta::FileInfo {
        let ExpiredAndMovedVersions {
            store,
            bucket,
            object,
            backend,
            expired,
            moved,
            ..
        } = case;
        let left = exact_versions(store, 0, bucket, object).await;
        assert!(
            left.is_empty(),
            "the source must keep neither a version nor a tier free version: {left:?}"
        );
        let mut owners = Vec::new();
        for target_pool_index in [1, 2] {
            owners.extend(exact_versions(store, target_pool_index, bucket, object).await);
        }
        assert_eq!(owners.len(), 2, "the moved version and one tier free version: {owners:?}");
        let (free_versions, live): (Vec<_>, Vec<_>) = owners.into_iter().partition(|version| version.tier_free_version());
        assert_eq!(live.len(), 1, "only the current version moves as a version: {live:?}");
        assert_eq!(live[0].version_id, moved.version_id);
        assert_same_remote_tuple(&live[0], moved);
        assert_eq!(
            free_versions.len(),
            1,
            "exactly one owner of the expired remote object: {free_versions:?}"
        );
        assert_same_remote_tuple(&free_versions[0], expired);
        for remote in [&expired.transitioned_objname, &moved.transitioned_objname] {
            assert!(backend.contains(remote).await, "no remote object may be deleted by the move");
        }
        assert_eq!(backend.remove_count().await, 0);
        free_versions[0].clone()
    }

    /// Of two transitioned versions in one entry, lifecycle expires the
    /// noncurrent one and the current one moves. The expiry is applied before
    /// the source cleanup, so the tier free version it leaves on the source
    /// moves with the entry instead of staying behind (rustfs/backlog#2873).
    #[test]
    #[serial_test::serial]
    fn decommission_moves_the_free_version_of_an_expired_version_with_its_entry() {
        run_large_stack_async_test("decommission-expired-and-moved-versions", || async {
            let case = put_expired_and_moved_transitioned_versions("expired-and-moved").await;
            let (store, bucket, object) = (&case.store, &case.bucket, case.object);
            // Load it the way the decommission routine does for each bucket.
            let lifecycle = crate::bucket::lifecycle::get_expiry_configs(store, bucket)
                .await
                .expect("the bucket lifecycle configuration should load")
                .lifecycle
                .map(|config| (*config).clone())
                .expect("the bucket should have a lifecycle configuration");

            start_transitioned_entry_decommission(store, bucket).await;
            // Busy expiry workers must not decide the outcome.
            let expiry_queue = crate::bucket::lifecycle::bucket_lifecycle_ops::HeldExpiryQueue::hold().await;
            let result = store
                .decommission_entry_for_test_with_lifecycle(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    bucket.clone(),
                    store.pools[0].get_disks_by_key(object),
                    Some(lifecycle),
                )
                .await;
            expiry_queue.release().await;
            result.expect("the entry should finish");

            assert_expired_version_owner_moved(&case).await;
        });
    }

    /// The full routine over the same entry: while the expiry workers are
    /// busy, the source cleanup used to delete the expired version first and
    /// leave its tier free version on the source, and the post-check then
    /// failed the decommission. It now completes in one pass, and the moved
    /// free version is consumed exactly once, after the cross-pool scan.
    #[test]
    #[serial_test::serial]
    fn decommission_of_an_expired_transitioned_version_completes_in_one_pass() {
        run_large_stack_async_test("decommission-expired-transitioned-routine", || async {
            let case = put_expired_and_moved_transitioned_versions("expired-routine").await;
            let (store, bucket) = (&case.store, &case.bucket);

            start_transitioned_entry_decommission(store, bucket).await;
            let expiry_queue = crate::bucket::lifecycle::bucket_lifecycle_ops::HeldExpiryQueue::hold().await;
            let routine = spawn_decommission_routine(store, 1)
                .await
                .expect("the decommission routine task should join");
            expiry_queue.release().await;
            routine.expect("the decommission should pass its post-check");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled,
                "complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert_eq!(info.items_decommission_failed, 0);
            drop(local);

            let free_version = assert_expired_version_owner_moved(&case).await;
            let owner = crate::object_api::ObjectInfo::from_file_info(&free_version, bucket, &free_version.name, true);
            assert!(
                crate::bucket::lifecycle::bucket_lifecycle_ops::cleanup_free_version_exact(
                    Arc::clone(store),
                    &owner,
                    &CancellationToken::new(),
                )
                .await
                .expect("the moved free version should be consumed"),
                "the moved free version must own the expired remote object"
            );
            assert!(!case.backend.contains(&case.expired.transitioned_objname).await);
            assert!(
                case.backend.contains(&case.moved.transitioned_objname).await,
                "the moved version keeps its remote object"
            );
            assert_eq!(case.backend.remove_count().await, 1, "the expired remote object is deleted exactly once");
            let mut left = Vec::new();
            for pool_index in [0, 1, 2] {
                left.extend(exact_versions(store, pool_index, bucket, case.object).await);
            }
            assert_eq!(left.len(), 1, "only the moved version remains: {left:?}");
            assert_eq!(left[0].version_id, case.moved.version_id);
        });
    }

    /// A transitioned version's expiry that cannot be applied leaves the
    /// version live: it moves to a target like any other version, so the
    /// entry neither fails nor leaves an owner of its remote object behind.
    #[test]
    #[serial_test::serial]
    fn decommission_moves_a_transitioned_version_whose_expiry_cannot_be_applied() {
        run_large_stack_async_test("decommission-expiry-not-applied", || async {
            let case = put_expired_and_moved_transitioned_versions("expiry-not-applied").await;
            let (store, bucket) = (&case.store, &case.bucket);

            start_transitioned_entry_decommission(store, bucket).await;
            let _fault = DecommissionTestFaultGuard::install(Arc::new(|stage, _, _, _, _| stage == "lifecycle_expiry_apply"));
            spawn_decommission_routine(store, 1)
                .await
                .expect("the decommission routine task should join")
                .expect("the decommission should pass its post-check");
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                info.complete && !info.failed && !info.canceled,
                "complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert_eq!(info.items_decommission_failed, 0);
            drop(local);

            assert_versions_moved_live(store, bucket, case.object, &case.backend, &[&case.expired, &case.moved]).await;
        });
    }

    /// The source holds nothing, and the target pools hold every source
    /// version as a live version with its own remote tuple. No tier free
    /// version was created and no remote object was deleted.
    async fn assert_versions_moved_live(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        backend: &crate::services::tier::test_util::MockWarmBackend,
        sources: &[&rustfs_filemeta::FileInfo],
    ) {
        let left = exact_versions(store, 0, bucket, object).await;
        assert!(left.is_empty(), "the source must keep nothing: {left:?}");
        let mut moved = Vec::new();
        for target_pool_index in [1, 2] {
            moved.extend(exact_versions(store, target_pool_index, bucket, object).await);
        }
        assert!(moved.iter().all(|version| !version.tier_free_version()), "{moved:?}");
        assert_eq!(moved.len(), sources.len(), "every version moves as a version: {moved:?}");
        for source in sources {
            let copy = moved
                .iter()
                .find(|version| version.version_id == source.version_id)
                .expect("each source version should move");
            assert_same_remote_tuple(copy, source);
            assert!(backend.contains(&source.transitioned_objname).await);
        }
        assert_eq!(backend.remove_count().await, 0);
    }

    /// An ExpiredObjectAllVersions rule expires a transitioned version with a
    /// prefix delete that needs the bucket lifecycle write lock. Applying it
    /// inline would wait on the entry's own lifecycle read lock until the
    /// lock timeout, and block every write in the bucket meanwhile. The
    /// version moves live instead, without waiting for that lock.
    #[test]
    #[serial_test::serial]
    fn decommission_moves_a_transitioned_version_expired_by_a_delete_all_rule_without_stalling() {
        run_large_stack_async_test("decommission-delete-all-transitioned", || async {
            let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("delete-all-transitioned");
            let object = "all-versions.bin";
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create the versioned bucket");
            let tier_name = format!("DECOMTIER{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let backend = register_mock_tier(&store.pools[0].instance_ctx().tier_config_mgr(), &tier_name).await;
            let now = time::OffsetDateTime::now_utc();
            put_transitioned_source_version(&store, &bucket, object, &tier_name, true, Some(now - time::Duration::days(10)))
                .await;
            let source = exact_versions(&store, 0, &bucket, object).await;
            assert_eq!(source.len(), 1);
            let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-all-versions</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix></Prefix>
    </Filter>
    <Expiration>
      <Days>1</Days>
      <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
    </Expiration>
  </Rule>
</LifecycleConfiguration>"#;
            crate::bucket::metadata_sys::update_in(
                &store.ctx,
                &bucket,
                crate::bucket::metadata::BUCKET_LIFECYCLE_CONFIG,
                lifecycle_xml.as_bytes().to_vec(),
            )
            .await
            .expect("store the delete-all expiration rule");
            let lifecycle = crate::bucket::lifecycle::get_expiry_configs(&store, &bucket)
                .await
                .expect("the bucket lifecycle configuration should load")
                .lifecycle
                .map(|config| (*config).clone())
                .expect("the bucket should have a lifecycle configuration");

            start_transitioned_entry_decommission(&store, &bucket).await;
            let started = std::time::Instant::now();
            store
                .decommission_entry_for_test_with_lifecycle(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    bucket.clone(),
                    store.pools[0].get_disks_by_key(object),
                    Some(lifecycle),
                )
                .await
                .expect("the entry should finish");
            let elapsed = started.elapsed();
            // The lock timeout is 5 seconds by default.
            assert!(
                elapsed < Duration::from_secs(4),
                "the entry must not wait for the bucket lifecycle write lock: {elapsed:?}"
            );

            assert_versions_moved_live(&store, &bucket, object, &backend, &[&source[0]]).await;
        });
    }

    /// The version is deleted between the entry's inventory and its lifecycle
    /// expiry, so the expiry is genuinely not applied. The entry does not
    /// fail: its retry moves the tier free version that the delete left.
    #[test]
    #[serial_test::serial]
    fn decommission_moves_the_free_version_left_when_the_expired_version_was_deleted_first() {
        run_large_stack_async_test("decommission-expired-version-deleted-first", || async {
            let case = put_expired_and_moved_transitioned_versions("expired-deleted-first").await;
            let (store, bucket, object) = (&case.store, &case.bucket, case.object);
            let lifecycle = crate::bucket::lifecycle::get_expiry_configs(store, bucket)
                .await
                .expect("the bucket lifecycle configuration should load")
                .lifecycle
                .map(|config| (*config).clone())
                .expect("the bucket should have a lifecycle configuration");

            start_transitioned_entry_decommission(store, bucket).await;
            let expiry_queue = crate::bucket::lifecycle::bucket_lifecycle_ops::HeldExpiryQueue::hold().await;
            let barrier = crate::core::pools::LifecycleDataMovementMutationBarrier::install(bucket, object);
            let entry = tokio::spawn({
                let (store, bucket) = (Arc::clone(store), bucket.clone());
                async move {
                    store
                        .decommission_entry_for_test_with_lifecycle(
                            0,
                            MetaCacheEntry {
                                name: object.to_string(),
                                ..Default::default()
                            },
                            bucket,
                            store.pools[0].get_disks_by_key(object),
                            Some(lifecycle),
                        )
                        .await
                }
            });
            barrier.wait_until_paused().await;
            // A client deletes the expired version first, so the expiry no
            // longer finds it. That delete leaves the version's tier free
            // version on the source.
            store.pools[0]
                .delete_object(
                    bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        version_id: case.expired.version_id.map(|version_id| version_id.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("delete the expired version");
            barrier.release();
            drop(barrier);
            let result = entry.await.expect("the entry task should join");
            expiry_queue.release().await;
            result.expect("an expiry that was not applied must not fail the entry");

            assert_expired_version_owner_moved(&case).await;
        });
    }

    /// The free-version worker consumes the expired version's free version
    /// as soon as the inline expiry creates it, before the entry moves it.
    /// The entry then finishes without it, and the remote object is deleted
    /// once.
    #[test]
    #[serial_test::serial]
    fn decommission_finishes_when_the_free_version_worker_consumes_the_expired_version_first() {
        run_large_stack_async_test("decommission-expired-free-version-consumed", || async {
            let case = put_expired_and_moved_transitioned_versions("expired-consumed").await;
            let (store, bucket, object) = (&case.store, &case.bucket, case.object);
            let lifecycle = crate::bucket::lifecycle::get_expiry_configs(store, bucket)
                .await
                .expect("the bucket lifecycle configuration should load")
                .lifecycle
                .map(|config| (*config).clone())
                .expect("the bucket should have a lifecycle configuration");

            start_transitioned_entry_decommission(store, bucket).await;
            let expiry_queue = crate::bucket::lifecycle::bucket_lifecycle_ops::HeldExpiryQueue::hold().await;
            let consumed = Arc::new(AtomicUsize::new(0));
            let _hook = crate::core::pools::DecommissionCleanupMutationGuard::install(Arc::new({
                let (store, consumed) = (Arc::clone(store), Arc::clone(&consumed));
                move |bucket: &str, object: &str, _attempt: usize| {
                    let (store, consumed, bucket, object) =
                        (Arc::clone(&store), Arc::clone(&consumed), bucket.to_string(), object.to_string());
                    Box::pin(async move {
                        for free_version in exact_versions(&store, 0, &bucket, &object)
                            .await
                            .into_iter()
                            .filter(|version| version.tier_free_version())
                        {
                            let owner =
                                crate::object_api::ObjectInfo::from_file_info(&free_version, &bucket, &free_version.name, true);
                            if crate::bucket::lifecycle::bucket_lifecycle_ops::cleanup_free_version_exact(
                                Arc::clone(&store),
                                &owner,
                                &CancellationToken::new(),
                            )
                            .await
                            .expect("the worker should consume the source free version")
                            {
                                consumed.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                }
            }));
            let result = store
                .decommission_entry_for_test_with_lifecycle(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    bucket.clone(),
                    store.pools[0].get_disks_by_key(object),
                    Some(lifecycle),
                )
                .await;
            expiry_queue.release().await;
            result.expect("the entry should finish");
            assert_eq!(
                consumed.load(Ordering::SeqCst),
                1,
                "the worker consumes the expired version's free version"
            );

            let left = exact_versions(store, 0, bucket, object).await;
            assert!(left.is_empty(), "the source must keep nothing: {left:?}");
            let mut owners = Vec::new();
            for target_pool_index in [1, 2] {
                owners.extend(exact_versions(store, target_pool_index, bucket, object).await);
            }
            assert_eq!(owners.len(), 1, "only the moved version remains: {owners:?}");
            assert_eq!(owners[0].version_id, case.moved.version_id);
            assert_same_remote_tuple(&owners[0], &case.moved);
            assert!(!case.backend.contains(&case.expired.transitioned_objname).await);
            assert!(case.backend.contains(&case.moved.transitioned_objname).await);
            assert_eq!(case.backend.remove_count().await, 1, "the expired remote object is deleted once");
        });
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn no_active_external_commit_probe_blocks_decommission_activation_until_commit_owns_it() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("no-active-probe");
        let object = "probe-target.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create no-active probe bucket");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                DecommissionPoolCapacityInfo::for_test(1, layout, 1024, 1024, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 1024, 1024, 0),
            ]],
        );
        let barrier = DecommissionCapacityLockOrderBarrier::install(store.id, store.id);
        barrier.pause_external_object_capacity_probe();
        let put_store = Arc::clone(&store);
        let put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(b"staged no-active commit".to_vec());
            put_store.pools[2]
                .put_object(
                    &bucket,
                    object,
                    &mut data,
                    &ObjectOptions {
                        decommission_capacity_admission: Some(Arc::clone(&put_store)),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_external_object_capacity_probe_acquired().await;

        let activation_store = Arc::clone(&store);
        let mut activation = tokio::spawn(async move {
            activation_store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut activation)
                .await
                .is_err(),
            "decommission activation must wait behind the no-active commit probe"
        );

        barrier.release_external_object_capacity_probe();
        tokio::time::timeout(std::time::Duration::from_secs(30), put)
            .await
            .expect("the staged external PUT should finish after probe release")
            .expect("the staged external PUT should not panic")
            .expect("the staged external PUT should commit");
        drop(barrier);
        tokio::time::timeout(std::time::Duration::from_secs(30), activation)
            .await
            .expect("decommission activation should proceed after the commit releases its probe")
            .expect("decommission activation should not panic")
            .expect("decommission activation should commit after the probe release");
    }

    #[test]
    #[serial_test::serial]
    fn staged_external_put_rechecks_retiring_source_on_another_node() {
        run_large_stack_current_thread_async_test("staged-retiring-source", || async {
            let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("staged-source");
            let object = "selected-before-retirement.bin";
            let original = b"original source object";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create staged source bucket");
            store.pools[0]
                .put_object(&bucket, object, &mut PutObjReader::from_vec(original.to_vec()), &ObjectOptions::default())
                .await
                .expect("seed the source selected before retirement");

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            set_decommission_capacity_info_overrides_for_test(
                other_store.id,
                vec![vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 4096, 4096, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, 0, 4096, 4096),
                ]],
            );
            let barrier = DecommissionCapacityLockOrderBarrier::install(store.id, store.id);
            barrier.pause_external_object_commit_phase();
            let put_store = Arc::clone(&store);
            let put_bucket = bucket.clone();
            let put = tokio::spawn(async move {
                put_store
                    .put_object(
                        &put_bucket,
                        object,
                        &mut PutObjReader::from_vec(b"must not replace a retiring source".to_vec()),
                        &ObjectOptions::default(),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_external_object_commit_phase_started())
                .await
                .expect("public PUT must stage before its decommission commit probe");
            assert!(!store.pool_meta.read().await.is_suspended(0));
            other_store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
                .expect("the other node should activate retirement before the staged PUT commits");
            assert!(other_store.pool_meta.read().await.is_suspended(0));
            assert!(
                !store.pool_meta.read().await.is_suspended(0),
                "the writer's local snapshot must remain stale to exercise the durable admission probe"
            );
            barrier.release_external_object_commit_phase();
            let result = tokio::time::timeout(Duration::from_secs(30), put)
                .await
                .expect("staged PUT must finish after the commit probe is released")
                .expect("staged PUT must not panic");
            assert!(
                matches!(result, Err(crate::error::Error::SlowDown)),
                "a staged PUT must retry pool selection instead of committing to a newly retiring source: {result:?}"
            );
            let mut reader = store.pools[0]
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the original source must remain readable after admission rejects the replacement");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("read the full retained source body");
            assert_eq!(body, original);
        });
    }

    #[test]
    #[serial_test::serial]
    fn reserved_target_shares_business_io_and_retains_source_after_capacity_loss() {
        run_large_stack_current_thread_async_test("shared-decommission-capacity", || async {
            for lose_capacity in [false, true] {
                let (_temp_dirs, store, other_store) =
                    test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
                let bucket = test_bucket("shared-capacity");
                let object = "migrating-source.bin";
                let business_object = "business-write.bin";
                let multipart_object = "business-multipart.bin";
                let source_body = vec![0x35; 256 * 1024];
                let business_body = vec![0x57; 64 * 1024];
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create shared-capacity bucket");
                store.pools[0]
                    .put_object(
                        &bucket,
                        object,
                        &mut PutObjReader::from_vec(source_body.clone()),
                        &ObjectOptions::default(),
                    )
                    .await
                    .expect("seed the retiring source");
                store.pools[2]
                    .put_object(
                        &bucket,
                        business_object,
                        &mut PutObjReader::from_vec(b"previous business value".to_vec()),
                        &ObjectOptions::default(),
                    )
                    .await
                    .expect("pin the public overwrite to the migration target");
                let multipart_opts = ObjectOptions {
                    expected_bucket_incarnation_id: Some(
                        store
                            .bucket_incarnation_id(&bucket)
                            .await
                            .expect("load the multipart bucket identity"),
                    ),
                    ..Default::default()
                };
                let routing_upload = new_multipart_upload(&store, 2, &bucket, multipart_object, multipart_opts.clone())
                    .await
                    .expect("pin subsequent public multipart creation to the migration target");
                let layout = DecommissionErasureLayout { data: 1, parity: 0 };
                let target_total = source_body.len() * 8;
                let capacities = vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_body.len() * 2, source_body.len() * 2),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                    DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
                ];
                set_decommission_capacity_info_overrides_for_test(store.id, vec![capacities.clone()]);
                store
                    .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                    .await
                    .expect("activate source retirement");
                *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
                let before = other_store.pool_meta.read().await.clone();
                let reservation = before.pools[0]
                    .decommission
                    .as_ref()
                    .expect("active source")
                    .capacity_reservation
                    .as_ref()
                    .expect("durable reservation");
                assert_eq!(
                    reservation.model_version, 2,
                    "exercise migration I/O outside the global metadata write lock"
                );
                assert_eq!(reservation.targets[0].pool_index, 2);

                let barrier =
                    crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
                let migration_store = Arc::clone(&store);
                let migration_bucket = bucket.clone();
                let migration = tokio::spawn(async move {
                    migration_store
                        .decommission_entry_for_test_with_bucket_incarnation(
                            0,
                            MetaCacheEntry {
                                name: object.to_string(),
                                ..Default::default()
                            },
                            migration_bucket,
                            migration_store.pools[0].get_disks_by_key(object),
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                    .await
                    .expect("migration must reach target publication");
                assert!(!migration.is_finished());
                let mut pending = crate::core::pools::PoolMeta::default();
                pending
                    .load_no_lock_from_replicas(other_store.pools.clone())
                    .await
                    .expect("read migration intent from the other node");
                let pending_reservation = pending.pools[0]
                    .decommission
                    .as_ref()
                    .expect("active source")
                    .capacity_reservation
                    .as_ref()
                    .expect("pending reservation")
                    .clone();
                assert_eq!(pending_reservation.pending_target_physical_bytes, source_body.len());
                assert_eq!(pending_reservation.consumed_target_physical_bytes, 0);

                tokio::time::timeout(
                    Duration::from_secs(30),
                    other_store.put_object(
                        &bucket,
                        business_object,
                        &mut PutObjReader::from_vec(business_body.clone()),
                        &ObjectOptions::default(),
                    ),
                )
                .await
                .expect("business PUT must finish without waiting for the migration target gate")
                .expect("a reserved healthy pool must accept ordinary PUT");
                tokio::time::timeout(Duration::from_secs(30), async {
                    let upload = other_store
                        .new_multipart_upload(&bucket, multipart_object, &ObjectOptions::default())
                        .await
                        .expect("the reserved target must accept public multipart creation");
                    assert_ne!(upload.upload_id, routing_upload.upload_id);
                    let lifecycle_guard = other_store
                        .acquire_bucket_lifecycle_read_lock(&bucket)
                        .await
                        .expect("fence the exact-pool multipart placement check");
                    let mut lookup_opts = multipart_opts.clone();
                    lookup_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
                    other_store.pools[2]
                        .get_multipart_info(&bucket, multipart_object, &upload.upload_id, &lookup_opts)
                        .await
                        .expect("public multipart creation must actually select the reserved target");
                    drop(lifecycle_guard);
                    let mut final_part = None;
                    for payload in [vec![0x18; business_body.len()], business_body.clone()] {
                        final_part = Some(
                            other_store
                                .put_object_part(
                                    &bucket,
                                    multipart_object,
                                    &upload.upload_id,
                                    1,
                                    &mut PutObjReader::from_vec(payload),
                                    &ObjectOptions::default(),
                                )
                                .await
                                .expect("the reserved target must accept UploadPart and replacement of the same part"),
                        );
                    }
                    let part = final_part.expect("the replacement part must be present");
                    Arc::clone(&other_store)
                        .complete_multipart_upload(
                            &bucket,
                            multipart_object,
                            &upload.upload_id,
                            vec![crate::storage_api_contracts::multipart::CompletePart {
                                part_num: part.part_num,
                                etag: part.etag,
                                ..Default::default()
                            }],
                            &ObjectOptions::default(),
                        )
                        .await
                        .expect("the reserved target must accept multipart completion");
                    other_store
                        .abort_multipart_upload(&bucket, multipart_object, &routing_upload.upload_id, &ObjectOptions::default())
                        .await
                        .expect("ordinary multipart cleanup must not consume the migration's pending intent");
                })
                .await
                .expect("business multipart operations must finish while migration I/O is paused");
                assert!(!migration.is_finished(), "business publication must overlap paused migration I/O");
                let mut after_business = crate::core::pools::PoolMeta::default();
                after_business
                    .load_no_lock_from_replicas(other_store.pools.clone())
                    .await
                    .expect("reload the shared-capacity ledger");
                assert_eq!(
                    after_business.pools[0]
                        .decommission
                        .as_ref()
                        .expect("active source")
                        .capacity_reservation
                        .as_ref(),
                    Some(&pending_reservation),
                    "ordinary PUT and multipart operations must not settle or consume the migration's pending identity"
                );

                let mut after_capacity = capacities;
                // Capacity injection is deterministic; the object I/O and durable metadata use real temporary disks.
                let free = if lose_capacity {
                    0
                } else {
                    target_total - source_body.len() - business_body.len() * 2
                };
                after_capacity[2] = DecommissionPoolCapacityInfo::for_test(2, layout, free, target_total, target_total - free);
                set_decommission_capacity_info_overrides_for_test(store.id, vec![after_capacity]);
                barrier.release();
                drop(barrier);
                let migrated = tokio::time::timeout(Duration::from_secs(30), migration)
                    .await
                    .expect("migration must finish after publication resumes")
                    .expect("migration task must not panic");
                if lose_capacity {
                    let err =
                        migrated.expect_err("capacity loss must prevent source cleanup, even after the target write commits");
                    assert!(err.to_string().contains("capacity"), "unexpected migration error: {err}");
                } else {
                    migrated.expect("shared-capacity migration should finish when space remains sufficient");
                }
                let mut persisted = crate::core::pools::PoolMeta::default();
                persisted
                    .load_no_lock_from_replicas(other_store.pools.clone())
                    .await
                    .expect("reload finalized migration state");
                let info = persisted.pools[0].decommission.as_ref().expect("source state");
                let reservation = info.capacity_reservation.as_ref().expect("migration ledger");
                assert_eq!(reservation.pending_target_physical_bytes, 0);
                assert_eq!(
                    reservation.consumed_target_physical_bytes,
                    source_body.len(),
                    "foreign writes must not count as committed source bytes"
                );
                assert_eq!(reservation.committed_data_bytes, source_body.len());
                assert_eq!(info.capacity_blocked_reason.is_some(), lose_capacity);
                for (pool, key, expected) in [
                    (2, business_object, &business_body),
                    (2, multipart_object, &business_body),
                    (2, object, &source_body),
                ] {
                    let mut reader = other_store.pools[pool]
                        .get_object_reader(&bucket, key, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("all acknowledged target objects must remain readable");
                    let mut actual = Vec::new();
                    reader.read_to_end(&mut actual).await.expect("read the complete target body");
                    assert_eq!(&actual, expected);
                }
                if lose_capacity {
                    let mut source = other_store.pools[0]
                        .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("capacity-blocked migration must retain its source");
                    let mut actual = Vec::new();
                    source
                        .read_to_end(&mut actual)
                        .await
                        .expect("read the complete retained source");
                    assert_eq!(actual, source_body);
                    other_store
                        .put_object(
                            &bucket,
                            business_object,
                            &mut PutObjReader::from_vec(business_body.clone()),
                            &ObjectOptions::default(),
                        )
                        .await
                        .expect("a capacity-blocked migration must not itself make the healthy target read-only");
                } else {
                    let err = other_store.pools[0]
                        .get_object_info(&bucket, object, &ObjectOptions::default())
                        .await
                        .expect_err("successful migration must clean the exact source");
                    assert!(crate::error::is_err_object_not_found(&err));
                }
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn mixed_batch_delete_admits_only_marker_destinations_during_retirement() {
        run_large_stack_current_thread_async_test("batch-marker-admission", || async {
            use crate::storage_api_contracts::object::ObjectToDelete;

            for marker_target in [1, 2] {
                let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
                let bucket = test_bucket("batch-marker");
                store
                    .make_bucket(
                        &bucket,
                        &MakeBucketOptions {
                            versioning_enabled: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("create a versioned batch-delete bucket");
                let source_version = uuid::Uuid::new_v4();
                for (pool, object, version) in [
                    (0, "purge-source", source_version),
                    (marker_target, "mark-active", uuid::Uuid::new_v4()),
                ] {
                    store.pools[pool]
                        .put_object(
                            &bucket,
                            object,
                            &mut PutObjReader::from_vec(b"version to delete".to_vec()),
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("seed each exact batch-delete destination");
                }
                let layout = DecommissionErasureLayout { data: 1, parity: 0 };
                set_decommission_capacity_info_overrides_for_test(
                    store.id,
                    vec![vec![
                        DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                        DecommissionPoolCapacityInfo::for_test(1, layout, 4096, 4096, 0),
                        DecommissionPoolCapacityInfo::for_test(2, layout, 0, 4096, 4096),
                    ]],
                );
                store
                    .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                    .await
                    .expect("reserve pool 1 while pool 0 retires and pool 2 remains unreserved");
                let (deleted, errors) = store
                    .delete_objects(
                        &bucket,
                        vec![
                            ObjectToDelete {
                                object_name: "mark-active".to_string(),
                                ..Default::default()
                            },
                            ObjectToDelete {
                                object_name: "purge-source".to_string(),
                                version_id: Some(source_version),
                                ..Default::default()
                            },
                        ],
                        ObjectOptions::default(),
                    )
                    .await;
                assert_eq!(errors.len(), 2);
                assert!(
                    errors.iter().all(Option::is_none),
                    "the unrelated retiring/reserved pools must not reject marker admission: {errors:?}"
                );
                assert_eq!(deleted.len(), 2);
                assert_eq!(deleted[0].object_name, "mark-active");
                assert!(deleted[0].delete_marker);
                assert!(
                    deleted[0].version_id.is_none(),
                    "a latest-version delete does not request an explicit version"
                );
                assert!(
                    deleted[0].delete_marker_version_id.is_some(),
                    "the newly created marker must have its own version identity"
                );
                assert_eq!(deleted[1].object_name, "purge-source");
                assert!(!deleted[1].delete_marker);
                assert_eq!(deleted[1].version_id, Some(source_version));
                assert!(
                    matches!(
                        store.pools[0]
                            .get_object_info(
                                &bucket,
                                "purge-source",
                                &ObjectOptions {
                                    version_id: Some(source_version.to_string()),
                                    ..Default::default()
                                },
                            )
                            .await,
                        Err(crate::error::Error::ObjectNotFound(..) | crate::error::Error::VersionNotFound(..))
                    ),
                    "an exact source deletion must retain its capacity-release path"
                );
            }
        });
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_upload_part_holds_decommission_capacity_until_rename() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-part-probe");
        let object = "public-part.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create public UploadPart probe bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load public UploadPart bucket incarnation");
        let upload = new_multipart_upload(
            &store,
            2,
            &bucket,
            object,
            ObjectOptions {
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("create public UploadPart probe upload");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                DecommissionPoolCapacityInfo::for_test(1, layout, 1024, 1024, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 1024, 1024, 0),
            ]],
        );
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::PutPartAfterCapacityAdmission);
        let put_store = Arc::clone(&other_store);
        let put_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(b"public staged part".to_vec());
            put_store
                .put_object_part(
                    &put_bucket,
                    object,
                    &upload_id,
                    1,
                    &mut data,
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;

        let activation_store = Arc::clone(&other_store);
        let mut activation = tokio::spawn(async move {
            activation_store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut activation)
                .await
                .is_err(),
            "decommission activation must wait behind the public UploadPart capacity guard"
        );

        barrier.release();
        let part = tokio::time::timeout(std::time::Duration::from_secs(30), put)
            .await
            .expect("public UploadPart should finish after the commit barrier release")
            .expect("public UploadPart task should not panic")
            .expect("public UploadPart should commit");
        drop(barrier);
        tokio::time::timeout(std::time::Duration::from_secs(30), activation)
            .await
            .expect("decommission activation should proceed after public UploadPart commit")
            .expect("decommission activation task should not panic")
            .expect("decommission activation should commit after public UploadPart");

        let listed = other_store
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("public UploadPart result should remain readable");
        assert_eq!(listed.parts.len(), 1, "public UploadPart should publish exactly one part");
        assert_eq!(listed.parts[0].part_num, part.part_num);
        assert_eq!(listed.parts[0].etag, part.etag);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_upload_part_stages_before_decommission_capacity_admission() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-part-staged");
        let object = "public-part-staged.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create staged public UploadPart bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load staged public UploadPart bucket incarnation");
        let upload = new_multipart_upload(
            &store,
            2,
            &bucket,
            object,
            ObjectOptions {
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("create staged public UploadPart upload");

        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::PutPartBeforeLockLost);
        let put_store = Arc::clone(&other_store);
        let put_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(b"public staged before admission".to_vec());
            put_store
                .put_object_part(
                    &put_bucket,
                    object,
                    &upload_id,
                    1,
                    &mut data,
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;

        let probe_store = Arc::clone(&other_store);
        let pool_meta_lock = probe_store
            .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
            .await
            .expect("create the public UploadPart capacity guard probe");
        let pool_meta_guard = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool_meta_lock.get_write_lock(std::time::Duration::from_secs(30)),
        )
        .await
        .expect("public UploadPart must not hold a capacity read guard during staging")
        .expect("public UploadPart capacity guard probe should acquire");
        drop(pool_meta_guard);

        barrier.release();
        let part = tokio::time::timeout(std::time::Duration::from_secs(30), put)
            .await
            .expect("staged public UploadPart should finish after the commit barrier release")
            .expect("staged public UploadPart task should not panic")
            .expect("staged public UploadPart should commit");
        drop(barrier);

        let listed = other_store
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("staged public UploadPart result should remain readable");
        assert_eq!(listed.parts.len(), 1, "staged public UploadPart should publish exactly one part");
        assert_eq!(listed.parts[0].part_num, part.part_num);
        assert_eq!(listed.parts[0].etag, part.etag);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn abandoned_reservation_does_not_hold_bucket_fence_behind_slow_peer() {
        use crate::set_disk::rename_fanout_barrier::{self, PHASE_RESERVATION_AFTER, PHASE_RESERVATION_BEFORE};

        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_dirs, store, other_store) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("abandoned-preparation");
            let object = "object.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            store.pools[2]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![1; 8193]),
                    &ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed object");
            let slow = rename_fanout_barrier::arm(object, 0, PHASE_RESERVATION_BEFORE);
            let ready = rename_fanout_barrier::arm(object, 1, PHASE_RESERVATION_AFTER);
            let worker = Arc::clone(&other_store);
            let task_bucket = bucket.clone();
            let put = tokio::spawn(async move {
                worker
                    .put_object(
                        &task_bucket,
                        object,
                        &mut PutObjReader::from_vec(vec![2; 8193]),
                        &ObjectOptions::default(),
                    )
                    .await
            });
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            slow.wait_until_paused_before(deadline).await;
            ready.wait_until_paused_before(deadline).await;
            tokio::time::timeout(Duration::from_secs(3), put)
                .await
                .expect("PUT fallback finishes")
                .expect("PUT task")
                .expect("PUT succeeds");
            ready.release();
            drop(ready);
            let lock = store
                .new_ns_lock(&bucket, rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH)
                .await
                .expect("publication lock");
            let writer = tokio::time::timeout(Duration::from_secs(2), lock.get_write_lock(Duration::from_secs(5))).await;
            slow.release();
            drop(slow);
            assert!(
                matches!(writer, Ok(Ok(_))),
                "abandoned completed tokens must not hold publication behind a slow peer"
            );
        })
        .await;
    }

    #[test]
    #[serial_test::serial]
    fn saturated_budget_keeps_following_same_key_puts_available() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(async {
                        use crate::set_disk::rename_fanout_barrier::{self, PHASE_RENAME};

                        temp_env::async_with_vars(
                            [(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))],
                            async {
                                let (_dirs, store, other_store) =
                                    test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
                                let bucket = test_bucket("speculative-budget-tail");
                                let object = "object.bin";
                                store
                                    .make_bucket(&bucket, &MakeBucketOptions::default())
                                    .await
                                    .expect("create bucket");
                                store.pools[2]
                                    .put_object(
                                        &bucket,
                                        object,
                                        &mut PutObjReader::from_vec(vec![1; 8193]),
                                        &ObjectOptions {
                                            write_completion: crate::object_api::WriteCompletion::TailDrained,
                                            ..Default::default()
                                        },
                                    )
                                    .await
                                    .expect("seed object");
                                let barrier = rename_fanout_barrier::arm(object, 0, PHASE_RENAME);
                                let worker = Arc::clone(&other_store);
                                let task_bucket = bucket.clone();
                                let first = tokio::spawn(async move {
                                    worker
                                        .put_object(
                                            &task_bucket,
                                            object,
                                            &mut PutObjReader::from_vec(vec![2; 8193]),
                                            &ObjectOptions::default(),
                                        )
                                        .await
                                });
                                barrier
                                    .wait_until_paused_before(tokio::time::Instant::now() + Duration::from_secs(10))
                                    .await;
                                tokio::time::timeout(Duration::from_secs(5), first)
                                    .await
                                    .expect("first PUT acknowledges")
                                    .expect("first task")
                                    .expect("first PUT succeeds");
                                let _permits = (0..crate::runtime::instance::SPECULATIVE_RESERVATION_LIMIT)
                                    .map(|_| other_store.ctx.try_acquire_speculative_reservation().expect("fill budget"))
                                    .collect::<Vec<_>>();
                                let staging = crate::set_disk::PutObjectCommitBarrier::install(
                                    &bucket,
                                    object,
                                    crate::set_disk::PutObjectCommitPause::BeforeBucketSnapshot,
                                );
                                let worker = Arc::clone(&other_store);
                                let task_bucket = bucket.clone();
                                let second_task = tokio::spawn(async move {
                                    worker
                                        .put_object(
                                            &task_bucket,
                                            object,
                                            &mut PutObjReader::from_vec(vec![3; 8193]),
                                            &ObjectOptions::default(),
                                        )
                                        .await
                                });
                                tokio::time::timeout(Duration::from_secs(5), staging.wait_until_paused())
                                    .await
                                    .expect("short reservation completes before staging");
                                tokio::time::sleep(Duration::from_secs(3)).await;
                                staging.release();
                                drop(staging);
                                let second = tokio::time::timeout(Duration::from_secs(7), second_task)
                                    .await
                                    .map(|result| result.expect("second task"));
                                let third = if matches!(second, Ok(Ok(_))) {
                                    Some(
                                        tokio::time::timeout(
                                            Duration::from_secs(7),
                                            other_store.put_object(
                                                &bucket,
                                                object,
                                                &mut PutObjReader::from_vec(vec![4; 8193]),
                                                &ObjectOptions::default(),
                                            ),
                                        )
                                        .await,
                                    )
                                } else {
                                    None
                                };
                                barrier.release();
                                tokio::task::yield_now().await;
                                barrier.release();
                                drop(barrier);
                                second.expect("second PUT completes").expect("second PUT succeeds");
                                third
                                    .expect("third PUT was attempted")
                                    .expect("third PUT completes")
                                    .expect("third PUT succeeds");
                            },
                        )
                        .await;
                    });
            })
            .expect("test thread")
            .join()
            .expect("test thread must not panic");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn saturated_speculative_budget_falls_back_then_readmits() {
        use crate::set_disk::rename_fanout_barrier::{self, PHASE_RESERVATION_BEFORE};

        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_dirs, store, other_store) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("speculative-budget");
            let object = "object.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            store.pools[2]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![1; 8193]),
                    &ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed object");
            let mut permits = (0..crate::runtime::instance::SPECULATIVE_RESERVATION_LIMIT)
                .map(|_| other_store.ctx.try_acquire_speculative_reservation().expect("fill budget"))
                .collect::<Vec<_>>();
            let barrier = rename_fanout_barrier::arm(object, 0, PHASE_RESERVATION_BEFORE);
            tokio::time::timeout(
                Duration::from_secs(3),
                other_store.put_object(&bucket, object, &mut PutObjReader::from_vec(vec![2; 8193]), &ObjectOptions::default()),
            )
            .await
            .expect("saturated preparation must not block")
            .expect("fallback PUT succeeds");
            assert!(barrier.is_paused(), "saturation must use the bounded short-lease path");
            drop(barrier);
            drop(permits.pop());
            let barrier = rename_fanout_barrier::arm(object, 0, PHASE_RESERVATION_BEFORE);
            let worker = Arc::clone(&other_store);
            let task_bucket = bucket.clone();
            let put = tokio::spawn(async move {
                worker
                    .put_object(
                        &task_bucket,
                        object,
                        &mut PutObjReader::from_vec(vec![3; 8193]),
                        &ObjectOptions::default(),
                    )
                    .await
            });
            barrier
                .wait_until_paused_before(tokio::time::Instant::now() + Duration::from_secs(5))
                .await;
            barrier.release();
            drop(barrier);
            tokio::time::timeout(Duration::from_secs(5), put)
                .await
                .expect("readmitted PUT completes")
                .expect("PUT task")
                .expect("PUT succeeds");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tentative_reservation_does_not_block_on_bucket_publication_writer() {
        let (_dirs, store, other_store) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("reservation-bucket-writer");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for key in ["baseline", "candidate"] {
            store.pools[2]
                .put_object(
                    &bucket,
                    key,
                    &mut PutObjReader::from_vec(vec![1; 256 * 1024]),
                    &ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed target pool");
        }
        let lock = store
            .new_ns_lock(&bucket, rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH)
            .await
            .expect("publication lock");
        let guard = lock
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("hold publication writer");
        let baseline =
            temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("false"))], async {
                tokio::time::timeout(
                    Duration::from_secs(2),
                    other_store.put_object(
                        &bucket,
                        "baseline",
                        &mut PutObjReader::from_vec(vec![2; 8193]),
                        &ObjectOptions::default(),
                    ),
                )
                .await
            })
            .await;
        let baseline = match baseline {
            Ok(Ok(result)) => result,
            result => {
                drop(guard);
                panic!("baseline did not complete with publication writer: {result:?}");
            }
        };
        assert!(baseline.etag.is_some());
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let mut data = PutObjReader::from_vec(vec![3; 8193]);
            let opts = ObjectOptions::default();
            let candidate = other_store.put_object(&bucket, "candidate", &mut data, &opts);
            tokio::pin!(candidate);
            let before = tokio::time::timeout(Duration::from_secs(2), &mut candidate).await;
            drop(guard);
            match before {
                Ok(result) => {
                    result.expect("candidate PUT succeeds");
                }
                Err(_) => {
                    let _ = tokio::time::timeout(Duration::from_secs(15), &mut candidate).await;
                    panic!("tentative reservations blocked the PUT behind a bucket publication writer");
                }
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn unrelated_pool_lock_does_not_double_put_selection_wait() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_dirs, store, other_store) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("unrelated-pool-lock");
            let object = "object.bin";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            store.pools[2]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![1; 256 * 1024]),
                    &ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed healthy pool");
            let unrelated_set = other_store.pools[1].get_disks_by_key(object);
            let lock = unrelated_set.new_ns_lock(&bucket, object).await.expect("unrelated pool lock");
            let guard = lock
                .get_write_lock(Duration::from_secs(5))
                .await
                .expect("hold unrelated pool lock");
            tokio::time::timeout(
                Duration::from_secs(1),
                other_store.pools[2].get_object_info(&bucket, object, &ObjectOptions::default()),
            )
            .await
            .expect("healthy pool uses a separate lock")
            .expect("healthy object exists");
            let worker = Arc::clone(&other_store);
            let put_bucket = bucket.clone();
            let mut put = tokio::spawn(async move {
                worker
                    .put_object(
                        &put_bucket,
                        object,
                        &mut PutObjReader::from_vec(vec![2; 256 * 1024]),
                        &ObjectOptions::default(),
                    )
                    .await
            });
            let before = tokio::time::timeout(Duration::from_secs(7), &mut put).await;
            drop(guard);
            match before {
                Ok(result) => {
                    result.expect("PUT task").expect("PUT succeeds in healthy pool");
                }
                Err(_) => {
                    let _ = tokio::time::timeout(Duration::from_secs(20), put).await;
                    panic!("the unrelated pool caused a second lock wait");
                }
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_orders_following_put_without_timeout() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_dirs, store, other_store) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("active-following-put");
            let object = "object.bin";
            let body = vec![0x71; 256 * 1024];
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            store.pools[2]
                .put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![0x69; body.len()]),
                    &ObjectOptions::default(),
                )
                .await
                .expect("seed target");
            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            let total = body.len().saturating_mul(4);
            set_decommission_capacity_info_overrides_for_test(
                store.id,
                vec![vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                    DecommissionPoolCapacityInfo::for_test(1, layout, total, total, 0),
                    DecommissionPoolCapacityInfo::for_test(2, layout, total, total, 0),
                ]],
            );
            store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
                .expect("activate decommission");
            *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
            let barrier =
                crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
            let first_store = Arc::clone(&other_store);
            let first_bucket = bucket.clone();
            let first = tokio::spawn(async move {
                first_store
                    .put_object(&first_bucket, object, &mut PutObjReader::from_vec(body), &ObjectOptions::default())
                    .await
            });
            barrier.wait_until_paused().await;
            first.await.expect("first task").expect("first PUT");
            let second = tokio::time::timeout(
                Duration::from_secs(7),
                other_store.put_object(
                    &bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![0x72; 256 * 1024]),
                    &ObjectOptions::default(),
                ),
            )
            .await;
            barrier.release();
            drop(barrier);
            second
                .expect("second PUT completes")
                .expect("second PUT succeeds during decommission");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_put_holds_decommission_capacity_through_early_ack_tail() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (_temp_dirs, store, other_store) =
                test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let bucket = test_bucket("public-put-tail");
            let object = "public-put-tail.bin";
            let body = vec![0x71; 256 * 1024];
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create public PUT tail bucket");

            let mut initial = PutObjReader::from_vec(vec![0x69; body.len()]);
            store.pools[2]
                .put_object(&bucket, object, &mut initial, &ObjectOptions::default())
                .await
                .expect("seed the unreserved public PUT target");

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            let target_total = body.len().saturating_mul(4);
            let capacity_snapshot = vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, target_total, target_total, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ];
            set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot]);
            store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
                .expect("activate the public PUT tail reservation");
            *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
            let reservation_target = store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("public PUT tail reservation should exist")
                .targets[0]
                .pool_index;
            assert_eq!(reservation_target, 1, "the public PUT target must remain unreserved");

            let tail_barrier =
                crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
            let put_store = Arc::clone(&other_store);
            let put_bucket = bucket.clone();
            let put_body = body.clone();
            let expected_body = body.clone();
            let mut put = tokio::spawn(async move {
                let mut data = PutObjReader::from_vec(put_body);
                put_store
                    .put_object(&put_bucket, object, &mut data, &ObjectOptions::default())
                    .await
            });
            tail_barrier.wait_until_paused().await;
            tokio::time::timeout(Duration::from_secs(30), &mut put)
                .await
                .expect("public PUT should return its early quorum acknowledgement")
                .expect("public PUT task should not panic")
                .expect("public PUT should commit its quorum");

            let capacity_lock = other_store
                .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
                .await
                .expect("create the public PUT capacity probe");
            let mut capacity_probe = tokio::spawn(async move { capacity_lock.get_write_lock(Duration::from_secs(30)).await });
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut capacity_probe)
                    .await
                    .is_err(),
                "public PUT early-ACK tail must retain its staged capacity read fence"
            );

            // Migration first takes a fixed-domain read anchor before copying
            // the source. It must not observe a partly drained foreground PUT.
            let source_lock = store
                .new_ns_lock(&bucket, object)
                .await
                .expect("create migration source probe");
            let (started, waiting) = tokio::sync::oneshot::channel();
            let mut source_probe = tokio::spawn(async move {
                let _ = started.send(());
                source_lock.get_read_lock(Duration::from_secs(30)).await
            });
            waiting.await.expect("migration source probe started");
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut source_probe)
                    .await
                    .is_err(),
                "migration source read must wait for the acknowledged PUT tail"
            );

            tail_barrier.release();
            drop(tail_barrier);
            let source_guard = tokio::time::timeout(Duration::from_secs(30), source_probe)
                .await
                .expect("migration source probe should finish after the tail")
                .expect("migration source probe should not panic")
                .expect("migration source probe should acquire after the tail");
            drop(source_guard);
            let capacity_guard = tokio::time::timeout(Duration::from_secs(30), capacity_probe)
                .await
                .expect("public PUT capacity probe should finish after its tail")
                .expect("public PUT capacity probe should not panic")
                .expect("public PUT capacity probe should acquire after its tail");
            drop(capacity_guard);

            let mut reader = other_store
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("public PUT result should remain readable");
            let mut committed = Vec::new();
            reader
                .stream
                .read_to_end(&mut committed)
                .await
                .expect("public PUT result body should be readable");
            assert_eq!(committed, expected_body, "public PUT should publish the requested bytes");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_put_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-put-lease-loss");
        let object = "public-put-lease-loss.bin";
        let body = vec![0x72; 256 * 1024];
        let old_body = vec![0x69; body.len()];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create public PUT lease-loss bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load public PUT lease-loss bucket incarnation");
        let mut old_target = PutObjReader::from_vec(old_body.clone());
        store.pools[2]
            .put_object(&bucket, object, &mut old_target, &ObjectOptions::default())
            .await
            .expect("seed the public PUT lease-loss target");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, target_total, target_total, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 0, target_total, target_total),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the public PUT lease-loss reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let barrier = PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::BeforeQuotaRename);
        tokio::time::pause();
        let put_store = Arc::clone(&lossy_store);
        let put_bucket = bucket.clone();
        let put_body = body.clone();
        let put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(put_body);
            put_store
                .put_object(
                    &put_bucket,
                    object,
                    &mut data,
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the public PUT capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), put)
            .await
            .expect("public PUT should finish after the commit barrier release")
            .expect("public PUT task should not panic")
            .expect_err("lost public PUT capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "public PUT lease loss should remain observable");
        drop(barrier);

        let mut target = lossy_store
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("the old public PUT target should remain readable");
        let mut committed = Vec::new();
        target
            .stream
            .read_to_end(&mut committed)
            .await
            .expect("the old public PUT target should drain");
        assert_eq!(committed, old_body, "lost public PUT capacity lease must preserve the prior target");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_cross_object_copy_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-copy-lease-loss");
        let source_object = "public-copy-source.bin";
        let target_object = "public-copy-target.bin";
        let source_body = vec![0x73; 256 * 1024];
        let old_target_body = vec![0x69; source_body.len()];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create public CopyObject lease-loss bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load public CopyObject lease-loss bucket incarnation");

        let mut source = PutObjReader::from_vec(source_body.clone());
        store.pools[2]
            .put_object(&bucket, source_object, &mut source, &ObjectOptions::default())
            .await
            .expect("seed the public CopyObject source");
        let mut old_target = PutObjReader::from_vec(old_target_body.clone());
        store.pools[2]
            .put_object(&bucket, target_object, &mut old_target, &ObjectOptions::default())
            .await
            .expect("seed the public CopyObject target");

        let mut copy_reader = store.pools[2]
            .get_object_reader(
                &bucket,
                source_object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the public CopyObject source");
        let mut copy_info = copy_reader.object_info.clone();
        let mut copy_body = Vec::new();
        copy_reader
            .stream
            .read_to_end(&mut copy_body)
            .await
            .expect("stage the public CopyObject source bytes");
        assert_eq!(copy_body, source_body);
        copy_info.put_object_reader = Some(PutObjReader::from_vec(copy_body));

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = source_body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_body.len(), source_body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, target_total, target_total, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 0, target_total, target_total),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the public CopyObject capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let barrier = PutObjectCommitBarrier::install(&bucket, target_object, PutObjectCommitPause::BeforeQuotaRename);
        tokio::time::pause();
        let copy_store = Arc::clone(&lossy_store);
        let copy_bucket = bucket.clone();
        let copy = tokio::spawn(async move {
            copy_store
                .copy_object(
                    &copy_bucket,
                    source_object,
                    &copy_bucket,
                    target_object,
                    &mut copy_info,
                    &ObjectOptions::default(),
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the public CopyObject capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), copy)
            .await
            .expect("public CopyObject should finish after the commit barrier release")
            .expect("public CopyObject task should not panic")
            .expect_err("lost public CopyObject capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "public CopyObject lease loss should remain observable");
        drop(barrier);
        tokio::time::resume();

        let mut target_reader = lossy_store.pools[2]
            .get_object_reader(&bucket, target_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the old public CopyObject target should remain readable");
        let mut committed = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut committed)
            .await
            .expect("the old public CopyObject target body should remain readable");
        assert_eq!(committed, old_target_body, "lost CopyObject capacity lease must not publish new bytes");
    }

    async fn assert_same_object_copy_fences_capacity_lease_loss(transitioned: bool) {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-self-copy-loss");
        let object = "public-self-copy-lease-loss.bin";
        let source_body = vec![0x74; 256 * 1024];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create public self-copy lease-loss bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load public self-copy lease-loss bucket incarnation");

        let (mut copy_info, source_opts, destination_opts, expected_body, expected_tier, expected_current_version) =
            if transitioned {
                let mut original = PutObjReader::from_vec(source_body.clone());
                let original_info = store.pools[2]
                    .put_object(&bucket, object, &mut original, &ObjectOptions::default())
                    .await
                    .expect("seed the transitioned self-copy source");
                let tier_name = format!("SELFCOPY{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
                let _backend = register_mock_tier(&store.pools[2].instance_ctx().tier_config_mgr(), &tier_name).await;
                store.pools[2]
                    .transition_object(
                        &bucket,
                        object,
                        &ObjectOptions {
                            no_lock: true,
                            transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                                status: TRANSITION_PENDING.to_string(),
                                tier: tier_name.clone(),
                                etag: original_info.etag.clone().expect("transition source should have an ETag"),
                                ..Default::default()
                            },
                            mod_time: original_info.mod_time,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed the transitioned self-copy source");
                let mut copy_info = store.pools[2]
                    .get_object_info(&bucket, object, &ObjectOptions::default())
                    .await
                    .expect("read the transitioned self-copy source metadata");
                assert_eq!(copy_info.transitioned_object.tier, tier_name);
                assert!(!copy_info.metadata_only, "transitioned self-copy must use the reader PUT path");
                let mut reader = store.pools[2]
                    .get_object_reader(
                        &bucket,
                        object,
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("read the transitioned self-copy source body");
                let mut copy_body = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut copy_body)
                    .await
                    .expect("stage the transitioned self-copy source body");
                assert_eq!(copy_body, source_body);
                copy_info.put_object_reader = Some(PutObjReader::from_vec(copy_body));
                (
                    copy_info,
                    ObjectOptions::default(),
                    ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                    source_body.clone(),
                    Some(tier_name),
                    None,
                )
            } else {
                let historical_version = uuid::Uuid::new_v4().to_string();
                let current_version = uuid::Uuid::new_v4().to_string();
                let current_body = vec![0x68; source_body.len()];
                let mut historical = PutObjReader::from_vec(source_body.clone());
                store.pools[2]
                    .put_object(
                        &bucket,
                        object,
                        &mut historical,
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(historical_version.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed the historical self-copy source");
                let mut current = PutObjReader::from_vec(current_body.clone());
                store.pools[2]
                    .put_object(
                        &bucket,
                        object,
                        &mut current,
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(current_version.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed the current self-copy target");
                let mut copy_info = store.pools[2]
                    .get_object_info(
                        &bucket,
                        object,
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(historical_version.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("read the historical self-copy source metadata");
                let mut reader = store.pools[2]
                    .get_object_reader(
                        &bucket,
                        object,
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            no_lock: true,
                            versioned: true,
                            version_id: Some(historical_version.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("read the historical self-copy source body");
                let mut copy_body = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut copy_body)
                    .await
                    .expect("stage the historical self-copy source body");
                assert_eq!(copy_body, source_body);
                copy_info.put_object_reader = Some(PutObjReader::from_vec(copy_body));
                (
                    copy_info,
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(historical_version),
                        ..Default::default()
                    },
                    ObjectOptions {
                        versioned: true,
                        expected_current_version_id: Some(current_version.clone()),
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                    current_body,
                    None,
                    Some(current_version),
                )
            };

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = source_body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_body.len(), source_body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, target_total, target_total, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the same-object CopyObject capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let barrier = PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::BeforeQuotaRename);
        tokio::time::pause();
        let copy_store = Arc::clone(&lossy_store);
        let copy_bucket = bucket.clone();
        let copy = tokio::spawn(async move {
            copy_store
                .copy_object(
                    &copy_bucket,
                    object,
                    &copy_bucket,
                    object,
                    &mut copy_info,
                    &source_opts,
                    &destination_opts,
                )
                .await
        });
        barrier.wait_until_paused().await;
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the same-object CopyObject capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), copy)
            .await
            .expect("same-object CopyObject should finish after the commit barrier release")
            .expect("same-object CopyObject task should not panic")
            .expect_err("lost same-object CopyObject capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "same-object CopyObject lease loss should remain observable");
        drop(barrier);
        tokio::time::resume();

        let target_info = lossy_store.pools[2]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("the failed same-object CopyObject target should remain readable");
        if let Some(expected_tier) = expected_tier {
            assert_eq!(
                target_info.transitioned_object.tier, expected_tier,
                "lost transitioned self-copy capacity lease must not publish a local target"
            );
        }
        if let Some(expected_current_version) = expected_current_version {
            assert_eq!(
                target_info.version_id.map(|version| version.to_string()),
                Some(expected_current_version),
                "lost historical self-copy capacity lease must not publish a new current version"
            );
        }
        let mut target_reader = store.pools[2]
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the failed same-object CopyObject target body should remain readable");
        let mut committed = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut committed)
            .await
            .expect("the failed same-object CopyObject target body should drain");
        assert_eq!(committed, expected_body, "lost same-object CopyObject lease must preserve target bytes");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_historical_self_copy_fences_capacity_lease_loss_before_publication() {
        assert_same_object_copy_fences_capacity_lease_loss(false).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_transitioned_self_copy_fences_capacity_lease_loss_before_publication() {
        assert_same_object_copy_fences_capacity_lease_loss(true).await;
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_multipart_restart_cleans_exact_zero_delta_upload_with_recovery_marker() {
        run_large_stack_async_test(
            "multipart-zero-delta-restart",
            data_movement_multipart_restart_cleans_exact_zero_delta_upload_with_recovery_marker_case,
        );
    }

    async fn data_movement_multipart_restart_cleans_exact_zero_delta_upload_with_recovery_marker_case() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-zero-delta-restart");
        let object = "staged-without-statfs-delta.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the zero-delta multipart restart bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the zero-delta multipart bucket incarnation");
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 8, 8),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 16, 16),
                DecommissionPoolCapacityInfo::for_test(2, layout, 32, 32, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, (0..8).map(|_| capacity_snapshot()).collect());
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the zero-delta multipart reservation");
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| reservation.targets.first())
            .expect("zero-delta fixture should reserve one target")
            .pool_index;
        assert_eq!(target_pool_index, 2);

        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        let exact_version = uuid::Uuid::new_v4();
        let foreign_version = uuid::Uuid::new_v4();
        let exact_mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(11);
        let foreign_mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(12);
        let exact_identity = format!("v1:{exact_version}:{}", exact_mod_time.unix_timestamp_nanos());
        let foreign_identity = format!("v1:{foreign_version}:{}", foreign_mod_time.unix_timestamp_nanos());
        let upload_opts = |identity: &str, version_id: uuid::Uuid, mod_time: time::OffsetDateTime| {
            let mut opts = ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                versioned: true,
                version_id: Some(version_id.to_string()),
                mod_time: Some(mod_time),
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            };
            rustfs_utils::http::insert_str(
                &mut opts.user_defined,
                rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
                identity.to_string(),
            );
            owner.apply_to(&mut opts);
            opts
        };
        let mut cleanup_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(exact_version.to_string()),
            mod_time: Some(exact_mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut cleanup_opts);

        store.reset_data_movement_multipart_discovery_count_for_test();
        store
            .reconcile_multipart_uploads_for_data_movement(target_pool_index, &bucket, object, &exact_identity, &cleanup_opts)
            .await
            .expect("a fresh mutation without recovery state should take the metadata fast path");
        assert_eq!(store.data_movement_multipart_discovery_count_for_test(), 0);

        let exact_upload_opts = upload_opts(&exact_identity, exact_version, exact_mod_time);
        let staging_store = Arc::clone(&store);
        let staging_bucket = bucket.clone();
        let exact_err = store
            .run_decommission_capacity_temporary_mutation(target_pool_index, Some(owner), Some(1), || async move {
                new_multipart_upload(&staging_store, target_pool_index, &staging_bucket, object, exact_upload_opts).await?;
                Err::<(), crate::error::Error>(crate::error::Error::OperationCanceled)
            })
            .await
            .expect_err("the fixture should fail after staging the exact zero-delta upload");
        assert!(matches!(exact_err, crate::error::Error::OperationCanceled));
        let foreign_upload = new_multipart_upload(
            &store,
            target_pool_index,
            &bucket,
            object,
            upload_opts(&foreign_identity, foreign_version, foreign_mod_time),
        )
        .await
        .expect("stage the foreign zero-delta upload");

        {
            let pool_meta = store.pool_meta.read().await;
            let reservation = pool_meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("zero-delta reservation should remain active");
            assert_eq!(reservation.pending_target_physical_bytes, 1);
            assert_eq!(reservation.inflight_target_physical_bytes, 0);
            assert_eq!(reservation.targets[0].pending_mutation_id, owner.mutation_id);
            assert_eq!(reservation.targets[0].temporary_mutations.len(), 1);
            assert_eq!(reservation.targets[0].temporary_mutations[0].mutation_id, owner.mutation_id.unwrap());
            assert_eq!(reservation.targets[0].temporary_mutations[0].physical_bytes, 0);
        }

        store
            .reconcile_multipart_uploads_for_data_movement(target_pool_index, &bucket, object, &exact_identity, &cleanup_opts)
            .await
            .expect("restart should remove the exact upload through its zero-byte recovery marker");
        assert_eq!(store.data_movement_multipart_discovery_count_for_test(), 1);

        let set = store.pools[target_pool_index].get_disks_by_key(object);
        let exact_remaining = set
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &exact_identity)
            .await
            .expect("scan for the exact zero-delta upload after reconciliation");
        assert!(exact_remaining.is_empty(), "the exact stale upload must be removed");
        let foreign_remaining = set
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &foreign_identity)
            .await
            .expect("scan for the foreign upload after reconciliation");
        assert_eq!(
            foreign_remaining,
            vec![foreign_upload.upload_id],
            "identity-scoped cleanup must preserve a foreign upload"
        );

        let pool_meta = store.pool_meta.read().await;
        let reservation = pool_meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("zero-delta reconciliation must preserve the reservation");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert!(reservation.targets[0].temporary_mutations.is_empty());

        drop(pool_meta);
        store
            .reconcile_multipart_uploads_for_data_movement(target_pool_index, &bucket, object, &exact_identity, &cleanup_opts)
            .await
            .expect("repeated cleanup should take the fast path after removing the marker");
        assert_eq!(store.data_movement_multipart_discovery_count_for_test(), 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn multipart_cleanup_rediscovers_uploads_and_proves_target_under_capacity_gate() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-cleanup-target-race");
        let object = "published-between-cleanup-proof-and-finalize.bin";
        let body = vec![0x5a; 64 * 1024];
        let version_id = uuid::Uuid::new_v4();
        let mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(19);
        let upload_identity = format!("v1:{version_id}:{}", mod_time.unix_timestamp_nanos());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the cleanup target-race bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the cleanup target-race bucket incarnation");
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, body.len().saturating_mul(2), body.len().saturating_mul(2)),
                DecommissionPoolCapacityInfo::for_test(2, layout, body.len().saturating_mul(2), body.len().saturating_mul(2), 0),
            ]],
        );
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the cleanup target-race reservation");
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| reservation.targets.first())
            .expect("cleanup target-race fixture should reserve one target")
            .pool_index;
        assert_eq!(target_pool_index, 2);
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        let mut upload_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id.to_string()),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        rustfs_utils::http::insert_str(
            &mut upload_opts.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity.clone(),
        );
        owner.apply_to(&mut upload_opts);
        let concurrent_upload_opts = upload_opts.clone();
        let mut cleanup_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id.to_string()),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut cleanup_opts);

        let staging_store = Arc::clone(&store);
        let staging_bucket = bucket.clone();
        store
            .run_decommission_capacity_temporary_mutation(target_pool_index, Some(owner), None, || async move {
                new_multipart_upload(&staging_store, target_pool_index, &staging_bucket, object, upload_opts).await?;
                Err::<(), crate::error::Error>(crate::error::Error::OperationCanceled)
            })
            .await
            .expect_err("the fixture should fail after staging its recoverable upload");
        store
            .run_decommission_capacity_temporary_mutation(target_pool_index, Some(owner), Some(body.len()), || async {
                Err::<(), crate::error::Error>(crate::error::Error::OperationCanceled)
            })
            .await
            .expect_err("the fixture should retain an exact pending capacity intent");
        {
            let pool_meta = store.pool_meta.read().await;
            let reservation = pool_meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("cleanup target-race reservation should remain active");
            assert!(reservation.pending_target_physical_bytes > 0);
            assert_eq!(reservation.targets[0].pending_mutation_id, owner.mutation_id);
            assert!(
                reservation.targets[0]
                    .temporary_mutations
                    .iter()
                    .any(|mutation| mutation.mutation_id == owner.mutation_id.unwrap())
            );
        }

        let target_lock = store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the cleanup target-race gate");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the cleanup target-race gate");
        let barrier = DecommissionCapacityLockOrderBarrier::install(store.id, store.id);
        barrier.disable_owner_pause();
        barrier.pause_target_gate_acquire(target_pool_index);
        let mut cleanup = tokio::spawn({
            let cleanup_store = Arc::clone(&store);
            let cleanup_bucket = bucket.clone();
            let cleanup_identity = upload_identity.clone();
            async move {
                cleanup_store
                    .reconcile_multipart_uploads_for_data_movement(
                        target_pool_index,
                        &cleanup_bucket,
                        object,
                        &cleanup_identity,
                        &cleanup_opts,
                    )
                    .await
            }
        });
        tokio::select! {
            _ = barrier.wait_until_target_gate_acquire_paused() => {}
            result = &mut cleanup => panic!("cleanup finished before its target-gate proof point: {result:?}"),
        }

        let concurrent_upload = new_multipart_upload(&store, target_pool_index, &bucket, object, concurrent_upload_opts)
            .await
            .expect("stage a same-identity upload after cleanup reaches its target-gate boundary");
        let mut target_data = PutObjReader::from_vec(body);
        let published = store.pools[target_pool_index]
            .put_object(
                &bucket,
                object,
                &mut target_data,
                &ObjectOptions {
                    data_movement: true,
                    src_pool_idx: 0,
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    mod_time: Some(mod_time),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("publish the exact owned target while cleanup waits for its capacity gate");
        assert!(crate::data_movement::is_owned_data_movement_target(&published));
        drop(target_guard);
        barrier.release_target_gate_acquire();
        tokio::time::timeout(Duration::from_secs(30), cleanup)
            .await
            .expect("cleanup should finish after acquiring its target gate")
            .expect("cleanup task should not panic")
            .expect("cleanup should preserve the now-published target intent");
        drop(barrier);

        let remaining_uploads = store.pools[target_pool_index]
            .get_disks_by_key(object)
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &upload_identity)
            .await
            .expect("check same-identity uploads after gated cleanup");
        assert!(
            remaining_uploads.is_empty(),
            "gated cleanup must rediscover and remove the concurrent same-identity upload {}",
            concurrent_upload.upload_id
        );

        let pool_meta = store.pool_meta.read().await;
        let reservation = pool_meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("cleanup target-race reservation should remain active");
        assert!(
            reservation.pending_target_physical_bytes > 0,
            "cleanup must not clear capacity for a target published before its gated proof"
        );
        assert_eq!(reservation.targets[0].pending_mutation_id, owner.mutation_id);
        assert!(reservation.targets[0].temporary_mutations.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_multipart_restart_reconciles_published_capacity_before_new_upload() {
        run_large_stack_current_thread_async_test(
            "multipart-published-capacity-restart",
            data_movement_multipart_restart_reconciles_published_capacity_before_new_upload_case,
        );
    }

    async fn data_movement_multipart_restart_reconciles_published_capacity_before_new_upload_case() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-restart");
        let object = "published-multipart-before-capacity-save.bin";
        let next_object = "next-after-published-multipart-reconcile.bin";
        let first_part = vec![0x61; 5 * 1024 * 1024];
        let second_part = vec![0x62; 1];
        let mut body = first_part.clone();
        body.extend_from_slice(&second_part);
        let source_version = uuid::Uuid::new_v4().to_string();
        let next_version = uuid::Uuid::new_v4().to_string();
        let source_mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(41);

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the multipart restart reconciliation bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the multipart restart reconciliation bucket incarnation");

        let source_upload = new_multipart_upload(
            &store,
            0,
            &bucket,
            object,
            ObjectOptions {
                versioned: true,
                version_id: Some(source_version.clone()),
                mod_time: Some(source_mod_time),
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("create the real source multipart upload");
        let source_multipart_opts = multipart_options(
            &store,
            &bucket,
            ObjectOptions {
                versioned: true,
                version_id: Some(source_version.clone()),
                mod_time: Some(source_mod_time),
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await;
        let mut source_parts = Vec::with_capacity(2);
        for (part_num, part_body) in [(1, first_part.as_slice()), (2, second_part.as_slice())] {
            let mut part_reader = PutObjReader::from_vec(part_body.to_vec());
            let part = store.pools[0]
                .put_object_part(
                    &bucket,
                    object,
                    &source_upload.upload_id,
                    part_num,
                    &mut part_reader,
                    &source_multipart_opts,
                )
                .await
                .expect("stage the real source multipart part");
            source_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        let source_info = store.pools[0]
            .clone()
            .complete_multipart_upload(&bucket, object, &source_upload.upload_id, source_parts, &source_multipart_opts)
            .await
            .expect("complete the real source multipart object");
        assert!(source_info.is_multipart(), "the source fixture must take the multipart migration path");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the source multipart capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);

        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the real source multipart object for migration");
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(lossy_store.id, (0..40).map(|_| capacity_snapshot()).collect());
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::AfterObjectPublication);
        let mut migration = tokio::spawn({
            let migration_store = Arc::clone(&lossy_store);
            let migration_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "multipart_restart_reconcile_initial",
                    Some(owner),
                )
                .await
            }
        });
        tokio::select! {
            () = barrier.wait_until_paused() => {}
            result = &mut migration => {
                panic!("initial multipart migration completed before reaching the publication barrier: {result:?}");
            }
        }
        tokio::time::pause();
        let published = lossy_store.pools[2]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the multipart target must publish before capacity progress save");
        assert!(published.is_multipart(), "published target must retain multipart identity");
        assert_eq!(published.version_id.map(|version| version.to_string()), Some(source_version.clone()));
        for prefix in [
            rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
        ] {
            assert_eq!(
                published
                    .user_defined
                    .get(&format!("{prefix}{}", rustfs_utils::http::SUFFIX_MULTIPART_UPLOAD_ID)),
                Some(&source_upload.upload_id),
                "migration must preserve the source completion identity under both internal prefixes"
            );
        }
        let mut published_reader = lossy_store.pools[2]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the published multipart target should be readable before reconciliation");
        let mut published_body = Vec::new();
        published_reader
            .stream
            .read_to_end(&mut published_body)
            .await
            .expect("the published multipart target body should be readable");
        assert_eq!(published_body, body, "the published multipart target must retain the full source body");
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the final multipart capacity save must observe its lost lease"
        );
        barrier.release();
        tokio::time::resume();
        let initial_err = tokio::time::timeout(Duration::from_secs(30), migration)
            .await
            .expect("the initial multipart migration should finish after publication release")
            .expect("the initial multipart migration task should not panic")
            .expect_err("the published multipart target must report its failed capacity progress save");
        assert!(
            !initial_err.to_string().is_empty(),
            "the failed multipart capacity save must remain observable"
        );
        drop(barrier);

        let mut failed_persisted = crate::core::pools::PoolMeta::default();
        failed_persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the published multipart failure must leave readable durable metadata");
        let failed_reservation = failed_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the failed multipart capacity intent must remain durable");
        assert_eq!(
            failed_reservation.pending_target_physical_bytes,
            body.len(),
            "the published target must retain its durable capacity intent for restart reconciliation"
        );
        assert_eq!(failed_reservation.inflight_target_physical_bytes, 0);
        assert_eq!(failed_reservation.consumed_target_physical_bytes, 0);
        *other_store.pool_meta.write().await = failed_persisted.clone();
        let retry_owner = decommission_capacity_owner(&failed_persisted);

        let new_upload_observation = NewMultipartUploadCommitObservation::install(&bucket, object);
        let retry_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("restart should reread the exact multipart source version");
        tokio::time::timeout(
            Duration::from_secs(30),
            data_movement::migrate_decommission_object(
                Arc::clone(&other_store),
                0,
                bucket.clone(),
                retry_reader,
                Some(incarnation),
                "multipart_restart_reconcile_retry",
                Some(retry_owner),
            ),
        )
        .await
        .expect("published multipart retry should not deadlock")
        .expect("published multipart retry should reconcile its durable capacity intent");
        assert!(
            !new_upload_observation.committed(),
            "published multipart retry must reconcile before starting a new MPU"
        );

        let mut reconciled = crate::core::pools::PoolMeta::default();
        reconciled
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reconciled multipart capacity metadata should remain durable");
        let reconciled_reservation = reconciled.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("reconciled multipart capacity reservation should remain present");
        assert_eq!(reconciled_reservation.pending_target_physical_bytes, 0);
        assert_eq!(reconciled_reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reconciled_reservation.consumed_target_physical_bytes, body.len());
        assert_eq!(reconciled_reservation.committed_data_bytes, body.len());

        let mut next_data = PutObjReader::from_vec(body.clone());
        other_store.pools[0]
            .put_object(
                &bucket,
                next_object,
                &mut next_data,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_version.clone()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the next source object for the multipart capacity oracle");
        let next_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                next_object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the next source object for the multipart capacity oracle");
        let next_result = tokio::time::timeout(
            Duration::from_secs(30),
            data_movement::migrate_decommission_object(
                Arc::clone(&other_store),
                0,
                bucket.clone(),
                next_reader,
                Some(incarnation),
                "multipart_restart_reconcile_next",
                Some(retry_owner),
            ),
        )
        .await
        .expect("the next-object multipart capacity oracle should finish promptly");
        assert!(next_result.is_err(), "consumed multipart capacity must reject the next exact-fit object");
        let next_target_err = other_store.pools[2]
            .get_object_info(
                &bucket,
                next_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_version),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("the next object must not publish after multipart reconciliation");
        assert!(
            crate::error::is_err_object_not_found(&next_target_err) || crate::error::is_err_version_not_found(&next_target_err),
            "capacity rejection must leave the next multipart target absent: {next_target_err}"
        );
    }

    /// Seeds a two-part null version, written before versioning was enabled,
    /// with a delete marker or a newer version above it, on pool 0
    /// (backlog#2873). Returns the null version's body and the seeded versions.
    async fn seed_multipart_null_version(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        incarnation: uuid::Uuid,
        marker_on_top: bool,
    ) -> (Vec<u8>, rustfs_filemeta::FileInfoVersions) {
        let first_part = vec![0x4e; 5 * 1024 * 1024];
        let second_part = vec![0x55; 1];
        let null_opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let upload = new_multipart_upload(store, 0, bucket, object, null_opts.clone())
            .await
            .expect("create the null-version upload");
        let multipart_opts = multipart_options(store, bucket, null_opts).await;
        let mut parts = Vec::with_capacity(2);
        for (part_num, part_body) in [(1, first_part.as_slice()), (2, second_part.as_slice())] {
            let mut reader = PutObjReader::from_vec(part_body.to_vec());
            let part = store.pools[0]
                .put_object_part(bucket, object, &upload.upload_id, part_num, &mut reader, &multipart_opts)
                .await
                .expect("stage a null-version part");
            parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        let null_info = store.pools[0]
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, parts, &multipart_opts)
            .await
            .expect("complete the null-version upload");
        assert!(null_info.is_multipart(), "the null version must use multipart data movement");

        let versioned = ObjectOptions {
            versioned: true,
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        if marker_on_top {
            store.pools[0]
                .delete_object(bucket, object, versioned)
                .await
                .expect("seed a delete marker over the null version");
        } else {
            store.pools[0]
                .put_object(
                    bucket,
                    object,
                    &mut PutObjReader::from_vec(b"written after versioning was enabled".to_vec()),
                    &versioned,
                )
                .await
                .expect("seed a newer version over the null version");
        }
        let seeded = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("read the seeded versions")
            .expect("the seeded versions must exist");
        assert_eq!(seeded.versions.len(), 2);
        assert!(seeded.versions[0].version_id.is_some());
        assert!(seeded.versions[1].version_id.is_none(), "the oldest version must be the null version");
        let mut body = first_part;
        body.extend_from_slice(&second_part);
        (body, seeded)
    }

    /// Both versions reached pool 1 with their identities, the null version's
    /// bytes are intact, and the source entry on pool 0 is gone.
    async fn assert_multipart_null_version_moved(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        body: &[u8],
        seeded: &rustfs_filemeta::FileInfoVersions,
    ) {
        let target = store.pools[1]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("read the target versions")
            .expect("both versions must reach the target pool");
        assert_eq!(target.versions.len(), 2, "{object}: both versions must move");
        assert_eq!(target.versions[0].version_id, seeded.versions[0].version_id);
        assert_eq!(target.versions[0].deleted, seeded.versions[0].deleted);
        assert!(
            target.versions[1].version_id.is_none(),
            "{object}: the null version must keep its identity"
        );
        assert_eq!(target.versions[1].mod_time, seeded.versions[1].mod_time);
        let mut reader = store.pools[1]
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: Some(uuid::Uuid::nil().to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("the moved null version must be readable");
        let mut actual = Vec::new();
        reader
            .stream
            .read_to_end(&mut actual)
            .await
            .expect("drain the moved null version");
        assert_eq!(
            rustfs_utils::crypto::hex_sha256(&actual, str::to_owned),
            rustfs_utils::crypto::hex_sha256(body, str::to_owned)
        );
        assert!(
            store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("read the source versions")
                .is_none(),
            "{object}: the source entry must be removed after both versions moved"
        );
    }

    fn multipart_null_version_capacity() -> Vec<DecommissionPoolCapacityInfo> {
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source = 16 * 1024 * 1024;
        let target = 256 * 1024 * 1024;
        vec![
            DecommissionPoolCapacityInfo::for_test(0, layout, 0, source, source),
            DecommissionPoolCapacityInfo::for_test(1, layout, target, target, 0),
        ]
    }

    /// The legacy (V1) capacity model runs the multipart staging cleanup proof
    /// on every attempt. That proof must look up the exact null version on the
    /// target; resolving the latest version found the marker or newer version
    /// moved first and failed every attempt.
    #[test]
    #[serial_test::serial]
    fn legacy_decommission_moves_multipart_null_version_behind_newer_versions() {
        run_large_stack_current_thread_async_test(
            "legacy-multipart-null-version",
            legacy_decommission_moves_multipart_null_version_behind_newer_versions_case,
        );
    }

    async fn legacy_decommission_moves_multipart_null_version_behind_newer_versions_case() {
        let (_temp_dirs, store, _other_store) =
            crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("legacy-mp-null");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the legacy multipart null-version bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the bucket incarnation");
        let mut seeded = Vec::new();
        for (object, marker_on_top) in [("behind-marker.bin", true), ("behind-newer.bin", false)] {
            let (body, versions) = seed_multipart_null_version(&store, &bucket, object, incarnation, marker_on_top).await;
            seeded.push((object, body, versions));
        }
        set_decommission_capacity_info_overrides_for_test(store.id, (0..40).map(|_| multipart_null_version_capacity()).collect());
        let _proof_guard = crate::services::notification_sys::without_decommission_target_fence_fleet_proof_for_test();
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate a legacy capacity reservation");
        let model_version = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the legacy reservation must exist")
            .model_version;
        assert_eq!(model_version, 1, "a proofless start must use the legacy capacity model");

        for (object, body, versions) in &seeded {
            store
                .decommission_entry_with_retry_state_for_test(
                    CancellationToken::new(),
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    bucket.clone(),
                    store.pools[0].get_disks_by_key(object),
                    Some(incarnation),
                    Arc::new(AtomicUsize::new(0)),
                )
                .await
                .unwrap_or_else(|err| panic!("{object}: the legacy multipart null version must move: {err:?}"));
            assert_multipart_null_version_moved(&store, &bucket, object, body, versions).await;
        }
    }

    /// Under the exact (V2) capacity model the staging cleanup proof runs when
    /// a previous attempt left a pending intent. An interrupted null-version
    /// upload must not block every later attempt.
    #[test]
    #[serial_test::serial]
    fn decommission_retries_interrupted_multipart_null_version_behind_newer_version() {
        run_large_stack_current_thread_async_test(
            "interrupted-multipart-null-version",
            decommission_retries_interrupted_multipart_null_version_behind_newer_version_case,
        );
    }

    async fn decommission_retries_interrupted_multipart_null_version_behind_newer_version_case() {
        let (_temp_dirs, store, other_store) =
            crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("interrupted-mp-null");
        let object = "interrupted-null.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the interrupted multipart null-version bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the bucket incarnation");
        let (body, seeded) = seed_multipart_null_version(&store, &bucket, object, incarnation, false).await;
        set_decommission_capacity_info_overrides_for_test(store.id, (0..40).map(|_| multipart_null_version_capacity()).collect());
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate an exact capacity reservation");
        assert_ne!(
            store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("the exact reservation must exist")
                .model_version,
            1,
            "a proven fleet must use the exact capacity model"
        );

        let part_barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::PutPartAfterRename);
        let interrupted = tokio::spawn({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let source_set = store.pools[0].get_disks_by_key(object);
            async move {
                store
                    .decommission_entry_with_retry_state_for_test(
                        CancellationToken::new(),
                        0,
                        MetaCacheEntry {
                            name: object.to_string(),
                            ..Default::default()
                        },
                        bucket,
                        source_set,
                        Some(incarnation),
                        Arc::new(AtomicUsize::new(0)),
                    )
                    .await
            }
        });
        part_barrier.wait_until_paused().await;
        interrupted.abort();
        let _ = interrupted.await;
        drop(part_barrier);

        // Resume on a fresh node context from the durable pool metadata, as a
        // restarted leader would.
        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reload the interrupted reservation");
        let pending = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the interrupted reservation must remain durable")
            .pending_target_physical_bytes;
        assert!(pending > 0, "the interrupted upload must leave a pending intent to clean up");
        *other_store.pool_meta.write().await = persisted;
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            (0..80).map(|_| multipart_null_version_capacity()).collect(),
        );
        other_store
            .decommission_entry_with_retry_state_for_test(
                CancellationToken::new(),
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.clone(),
                other_store.pools[0].get_disks_by_key(object),
                Some(incarnation),
                Arc::new(AtomicUsize::new(0)),
            )
            .await
            .expect("the retry must clean up the interrupted upload and move the null version");
        assert_multipart_null_version_moved(&other_store, &bucket, object, &body, &seeded).await;
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_multipart_restart_cleans_partial_upload_before_exact_fit_retry() {
        run_large_stack_current_thread_async_test(
            "multipart-partial-upload-restart",
            data_movement_multipart_restart_cleans_partial_upload_before_exact_fit_retry_case,
        );
    }

    async fn data_movement_multipart_restart_cleans_partial_upload_before_exact_fit_retry_case() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-part-restart");
        let object = "published-part-before-capacity-save.bin";
        let first_part = vec![0x71; 5 * 1024 * 1024];
        let second_part = vec![0x72; 1];
        let mut body = first_part.clone();
        body.extend_from_slice(&second_part);
        let source_version = uuid::Uuid::new_v4().to_string();
        let source_mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(71);

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the partial multipart restart bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the partial multipart restart bucket incarnation");
        let source_upload = new_multipart_upload(
            &store,
            0,
            &bucket,
            object,
            ObjectOptions {
                versioned: true,
                version_id: Some(source_version.clone()),
                mod_time: Some(source_mod_time),
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("create the partial-restart source upload");
        let source_multipart_opts = multipart_options(
            &store,
            &bucket,
            ObjectOptions {
                versioned: true,
                version_id: Some(source_version.clone()),
                mod_time: Some(source_mod_time),
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await;
        let mut source_parts = Vec::with_capacity(2);
        for (part_num, part_body) in [(1, first_part.as_slice()), (2, second_part.as_slice())] {
            let mut reader = PutObjReader::from_vec(part_body.to_vec());
            let part = store.pools[0]
                .put_object_part(&bucket, object, &source_upload.upload_id, part_num, &mut reader, &source_multipart_opts)
                .await
                .expect("stage the partial-restart source part");
            source_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        let source_info = store.pools[0]
            .clone()
            .complete_multipart_upload(&bucket, object, &source_upload.upload_id, source_parts, &source_multipart_opts)
            .await
            .expect("complete the partial-restart source object");
        assert!(source_info.is_multipart(), "the fixture must use multipart data movement");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source_estimate = body.len().saturating_add(1);
        let target_total = source_estimate.saturating_mul(2);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_estimate, source_estimate),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the exact-fit partial multipart reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the partial-restart source object");
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(lossy_store.id, (0..40).map(|_| capacity_snapshot()).collect());
        let part_barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::PutPartAfterRename);
        let abort_barrier = data_movement::DataMovementMultipartAbortBarrier::install(&bucket, object);
        let migration = tokio::spawn({
            let migration_store = Arc::clone(&lossy_store);
            let migration_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "multipart_partial_restart_initial",
                    Some(owner),
                )
                .await
            }
        });
        part_barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the published UploadPart capacity save must lose its lease"
        );
        part_barrier.release();
        abort_barrier.wait_until_paused().await;
        migration.abort();
        let _ = migration.await;
        drop(abort_barrier);
        drop(part_barrier);
        tokio::time::resume();

        let upload_identity = format!("v1:{source_version}:{}", source_mod_time.unix_timestamp_nanos());
        let stale_uploads = lossy_store.pools[2]
            .get_disks_by_key(object)
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &upload_identity)
            .await
            .expect("discover the published partial upload after the simulated crash");
        assert_eq!(stale_uploads.len(), 1, "the failed UploadPart must leave exactly one recoverable upload");

        let mut failed_persisted = crate::core::pools::PoolMeta::default();
        failed_persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("load the durable partial UploadPart capacity intent");
        let failed_reservation = failed_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the partial UploadPart reservation should remain durable");
        assert!(failed_reservation.pending_target_physical_bytes > 0);
        assert_eq!(failed_reservation.consumed_target_physical_bytes, 0);
        *other_store.pool_meta.write().await = failed_persisted.clone();
        set_decommission_capacity_info_overrides_for_test(other_store.id, (0..80).map(|_| capacity_snapshot()).collect());
        let retry_owner = decommission_capacity_owner(&failed_persisted);
        let retry_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("restart should reread the exact partial multipart source");
        let new_upload = NewMultipartUploadCommitObservation::install(&bucket, object);
        let retry_barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::NewUploadBeforeLockLost);
        let mut retry = tokio::spawn({
            let retry_store = Arc::clone(&other_store);
            let retry_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    retry_store,
                    0,
                    retry_bucket,
                    retry_reader,
                    Some(incarnation),
                    "multipart_partial_restart_retry",
                    Some(retry_owner),
                )
                .await
            }
        });
        tokio::select! {
            _ = retry_barrier.wait_until_paused() => {}
            result = &mut retry => {
                panic!("partial multipart retry completed before reaching the new-upload barrier: {result:?}");
            }
        }
        let uploads_before_retry = other_store.pools[2]
            .get_disks_by_key(object)
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &upload_identity)
            .await
            .expect("scan for the old partial upload before the replacement upload commits");
        assert!(
            uploads_before_retry.is_empty(),
            "restart must delete the old partial upload before publishing a replacement upload"
        );
        retry_barrier.release();
        tokio::time::timeout(Duration::from_secs(30), retry)
            .await
            .expect("the exact-fit partial multipart retry should not deadlock")
            .expect("the exact-fit partial multipart retry task should not panic")
            .expect("the exact-fit partial multipart retry should converge");
        drop(retry_barrier);
        assert!(new_upload.committed(), "restart must create a fresh upload after cleaning the old one");
        let remaining_uploads = other_store.pools[2]
            .get_disks_by_key(object)
            .data_movement_multipart_upload_ids(&bucket, object, Some(incarnation), &upload_identity)
            .await
            .expect("scan for residual partial uploads after restart convergence");
        assert!(
            remaining_uploads.is_empty(),
            "restart must remove the original partial upload before retrying"
        );

        let mut reconciled = crate::core::pools::PoolMeta::default();
        reconciled
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reload exact-fit multipart progress after restart");
        let reservation = reconciled.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the exact-fit multipart reservation should remain readable");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reservation.consumed_target_physical_bytes, body.len());
        assert_eq!(reservation.committed_data_bytes, body.len());

        let mut target = other_store.pools[2]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the exact-fit retry target should be readable");
        let mut target_body = Vec::new();
        target
            .stream
            .read_to_end(&mut target_body)
            .await
            .expect("read the exact-fit retry target body");
        assert_eq!(target_body, body);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_abort_fences_capacity_lease_loss_before_delete() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-abort-fence");
        let object = "abort-capacity-lease-before-delete.bin";
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 2, 2),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 4, 4),
                DecommissionPoolCapacityInfo::for_test(2, layout, 4, 4, 0),
            ]
        };
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the abort fence bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the abort fence incarnation");
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the abort fence reservation");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        let version_id = uuid::Uuid::new_v4().to_string();
        let mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(83);
        let mut upload_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id.clone()),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let upload_identity = data_movement::data_movement_upload_identity_from_options(&upload_opts);
        rustfs_utils::http::insert_str(
            &mut upload_opts.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity,
        );
        let upload = new_multipart_upload(&store, 2, &bucket, object, upload_opts)
            .await
            .expect("create the abort fence staging upload");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(lossy_store.id, vec![capacity_snapshot(), capacity_snapshot()]);
        let mut abort_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut abort_opts);
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::AbortBeforeDelete);
        tokio::time::pause();
        let abort = tokio::spawn({
            let abort_store = Arc::clone(&lossy_store);
            let abort_bucket = bucket.clone();
            let abort_upload_id = upload.upload_id.clone();
            let abort_opts = abort_opts.clone();
            async move {
                abort_store
                    .abort_multipart_upload_for_data_movement(2, &abort_bucket, object, &abort_upload_id, &abort_opts)
                    .await
            }
        });
        barrier.wait_until_paused().await;
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the abort delete must observe capacity lease refresh loss"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), abort)
            .await
            .expect("the fenced abort should finish after release")
            .expect("the fenced abort task should not panic")
            .expect_err("lost capacity lease must reject the staging delete");
        assert!(!err.to_string().is_empty());
        drop(barrier);
        tokio::time::resume();

        let upload_info = lossy_store.pools[2]
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    data_movement: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("the fenced abort must leave the staging upload readable");
        assert!(upload_info.parts.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn unversioned_decommission_multipart_uses_reserved_target() {
        run_large_stack_current_thread_async_test(
            "unversioned-decommission-multipart-target",
            unversioned_decommission_multipart_uses_reserved_target_case,
        );
    }

    async fn unversioned_decommission_multipart_uses_reserved_target_case() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("unversioned-decommission-multipart-target");
        let object = "unversioned-existing-unreserved-target.bin";
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1, 1),
                DecommissionPoolCapacityInfo::for_test(1, layout, 2, 2, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 100, 100, 0),
            ]
        };

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the unversioned multipart target bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the unversioned multipart target incarnation");

        // Force the ordinary pool selector to choose pool 2. A capacity-owned
        // upload must ignore this unreserved existing object and use pool 1.
        let mut existing = PutObjReader::from_vec(b"existing unreserved target".to_vec());
        store.pools[2]
            .put_object(
                &bucket,
                object,
                &mut existing,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the unreserved target object");

        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the unversioned multipart reservation");
        {
            let pool_meta = store.pool_meta.read().await;
            let targets = &pool_meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("the unversioned multipart reservation should exist")
                .targets;
            assert_eq!(targets.len(), 1, "only pool 1 should be reserved");
            assert_eq!(targets[0].pool_index, 1);
        }

        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        let mut upload_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: false,
            version_id: None,
            mod_time: Some(time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(97)),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut upload_opts);

        let (_, target_pool_idx, _) = store
            .handle_new_multipart_upload_with_pool_idx(&bucket, object, &upload_opts, None)
            .await
            .expect("unversioned decommission multipart must use the reserved target");
        assert_eq!(target_pool_idx, 1, "the reservation target must override the existing unreserved object");
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_multipart_abort_restart_reconciles_inflight_after_release_save_loss() {
        run_large_stack_current_thread_async_test(
            "multipart-abort-restart",
            data_movement_multipart_abort_restart_reconciles_inflight_after_release_save_loss_case,
        );
    }

    async fn data_movement_multipart_abort_restart_reconciles_inflight_after_release_save_loss_case() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("multipart-abort-restart");
        let object = "deleted-upload-before-release-save.bin";
        let body = vec![0x79; 256 * 1024];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source_estimate = body.len().saturating_add(1);
        let target_total = source_estimate.saturating_mul(2);
        let capacity_snapshot = |target_free| {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_estimate, source_estimate),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_free, target_total, target_total - target_free),
            ]
        };
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the abort restart bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the abort restart incarnation");
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot(target_total)]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the abort restart reservation");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await).with_mutation_id(uuid::Uuid::new_v4());
        let version_id = uuid::Uuid::new_v4().to_string();
        let mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(91);
        let mut new_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id.clone()),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let upload_identity = data_movement::data_movement_upload_identity_from_options(&new_opts);
        rustfs_utils::http::insert_str(
            &mut new_opts.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity,
        );
        owner.apply_to(&mut new_opts);
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![
                capacity_snapshot(target_total),
                capacity_snapshot(target_total),
                capacity_snapshot(target_total - 1),
            ],
        );
        let (upload, target_pool_idx, _) = store
            .handle_new_multipart_upload_with_pool_idx(&bucket, object, &new_opts, None)
            .await
            .expect("create the capacity-accounted data movement upload");
        assert_eq!(target_pool_idx, 2);

        let mut part_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            part_number: Some(1),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut part_opts);
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![
                capacity_snapshot(target_total - 1),
                capacity_snapshot(target_total - 1),
                capacity_snapshot(target_total - 1 - body.len()),
            ],
        );
        let mut part_reader = PutObjReader::from_vec(body.clone());
        store
            .put_object_part_for_data_movement(target_pool_idx, &bucket, object, &upload.upload_id, &mut part_reader, &part_opts)
            .await
            .expect("persist the scoped temporary multipart inflight bytes");
        let persisted_before_abort = store.pool_meta.read().await.clone();
        let before_reservation = persisted_before_abort.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the abort fixture reservation should exist");
        assert_eq!(before_reservation.pending_target_physical_bytes, 0);
        assert_eq!(before_reservation.inflight_target_physical_bytes, body.len() + 1);
        assert_eq!(before_reservation.targets[0].temporary_mutations.len(), 1);
        assert_eq!(
            before_reservation.targets[0].temporary_mutations[0].mutation_id,
            owner
                .mutation_id
                .expect("the abort fixture owner should have a mutation identity")
        );
        *other_store.pool_meta.write().await = persisted_before_abort;

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![
                capacity_snapshot(target_total - 1 - body.len()),
                capacity_snapshot(target_total - 1 - body.len()),
            ],
        );
        let mut abort_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut abort_opts);
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::AbortAfterDelete);
        let abort = tokio::spawn({
            let abort_store = Arc::clone(&lossy_store);
            let abort_bucket = bucket.clone();
            let abort_object = object.to_string();
            let abort_upload_id = upload.upload_id.clone();
            let abort_opts = abort_opts.clone();
            async move {
                abort_store
                    .abort_multipart_upload_for_data_movement(
                        target_pool_idx,
                        &abort_bucket,
                        &abort_object,
                        &abort_upload_id,
                        &abort_opts,
                    )
                    .await
            }
        });
        barrier.wait_until_paused().await;
        // Native cleanup performs real filesystem I/O. Pause time only after
        // durable deletion, when the abort is blocked before its release save.
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the abort release save must observe capacity lease loss after deletion"
        );
        barrier.release();
        let abort_err = tokio::time::timeout(Duration::from_secs(30), abort)
            .await
            .expect("the fenced abort should finish after release")
            .expect("the fenced abort task should not panic")
            .expect_err("the deleted upload must report its lost final capacity save");
        assert!(!abort_err.to_string().is_empty());
        drop(barrier);
        tokio::time::resume();

        let missing = lossy_store.pools[target_pool_idx]
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("the first abort must durably delete the staging upload");
        assert!(crate::error::is_err_invalid_upload_id(&missing));
        let mut failed_persisted = crate::core::pools::PoolMeta::default();
        failed_persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("reload inflight capacity after the abort release save loss");
        let failed_reservation = failed_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the failed abort reservation should remain durable");
        assert_eq!(failed_reservation.inflight_target_physical_bytes, body.len() + 1);
        assert_eq!(failed_reservation.targets[0].temporary_mutations.len(), 1);
        assert_eq!(
            failed_reservation.targets[0].temporary_mutations[0].mutation_id,
            owner
                .mutation_id
                .expect("the failed abort owner should retain its mutation identity")
        );

        *other_store.pool_meta.write().await = failed_persisted;
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![
                capacity_snapshot(target_total - 1 - body.len()),
                capacity_snapshot(target_total - 1 - body.len()),
            ],
        );
        other_store
            .abort_multipart_upload_for_data_movement(target_pool_idx, &bucket, object, &upload.upload_id, &abort_opts)
            .await
            .expect("restart must treat the missing upload as a confirmed scoped release");
        let mut reconciled = crate::core::pools::PoolMeta::default();
        reconciled
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reload capacity after idempotent abort reconciliation");
        let reservation = reconciled.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the reconciled abort reservation should remain readable");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert!(reservation.targets[0].temporary_mutations.is_empty());
        assert_eq!(reservation.consumed_target_physical_bytes, 0);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_complete_multipart_fences_inner_capacity_lease_loss() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("public-complete-loss");
        let object = "public-complete-lease-loss.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create public CompleteMultipartUpload lease-loss bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load public CompleteMultipartUpload bucket incarnation");
        let upload = new_multipart_upload(
            &store,
            2,
            &bucket,
            object,
            ObjectOptions {
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await
        .expect("create public CompleteMultipartUpload lease-loss upload");
        let multipart_opts = multipart_options(
            &store,
            &bucket,
            ObjectOptions {
                expected_bucket_incarnation_id: Some(incarnation),
                ..Default::default()
            },
        )
        .await;
        let body = b"public complete staged part".to_vec();
        let mut data = PutObjReader::from_vec(body.clone());
        let part = store.pools[2]
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut data, &multipart_opts)
            .await
            .expect("stage public CompleteMultipartUpload lease-loss part");
        let completed_parts = vec![crate::storage_api_contracts::multipart::CompletePart {
            part_num: part.part_num,
            etag: part.etag,
            ..Default::default()
        }];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, target_total, target_total, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 0, target_total, target_total),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the public CompleteMultipartUpload capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::BeforeQuotaRename);
        tokio::time::pause();
        let complete_store = Arc::clone(&lossy_store);
        let complete_bucket = bucket.clone();
        let complete_upload_id = upload.upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(
                    &complete_bucket,
                    object,
                    &complete_upload_id,
                    completed_parts,
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the inner CompleteMultipartUpload capacity guard must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), complete)
            .await
            .expect("public CompleteMultipartUpload should finish after the commit barrier release")
            .expect("public CompleteMultipartUpload task should not panic")
            .expect_err("lost inner capacity lease must fence CompleteMultipartUpload before rename");
        assert!(
            err.to_string().contains("quota_reservation"),
            "inner capacity lease loss should report a quota reservation fence error: {err}"
        );
        drop(barrier);

        let object_err = lossy_store
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect_err("failed CompleteMultipartUpload must not publish the object");
        assert!(
            crate::error::is_err_object_not_found(&object_err),
            "failed CompleteMultipartUpload should leave the target object absent: {object_err}"
        );
        lossy_store
            .get_multipart_info(
                &bucket,
                object,
                &upload.upload_id,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("failed CompleteMultipartUpload must leave the MPU retryable");
        let listed = lossy_store
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("failed CompleteMultipartUpload should preserve the staged part");
        assert_eq!(listed.parts.len(), 1, "failed CompleteMultipartUpload must preserve the MPU part");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_complete_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let object = "data-movement-complete-lease-loss.bin";
        let body = vec![0x4d; 64];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 4096, 4096),
                DecommissionPoolCapacityInfo::for_test(2, layout, 4096, 4096, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the data-movement capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let reserved_target = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(reserved_target, 2, "the exact-fit data-movement target must be pool 2");

        let upload = new_multipart_upload(
            &store,
            2,
            RUSTFS_META_BUCKET,
            object,
            ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                ..Default::default()
            },
        )
        .await
        .expect("create the staged data-movement upload");
        let multipart_opts = multipart_options(
            &store,
            RUSTFS_META_BUCKET,
            ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                ..Default::default()
            },
        )
        .await;
        let mut data = PutObjReader::from_vec(body.clone());
        let part = store.pools[2]
            .put_object_part(RUSTFS_META_BUCKET, object, &upload.upload_id, 1, &mut data, &multipart_opts)
            .await
            .expect("stage the data-movement completion part");
        let completed_parts = vec![crate::storage_api_contracts::multipart::CompletePart {
            part_num: part.part_num,
            etag: part.etag,
            ..Default::default()
        }];

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let mut complete_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        owner.apply_to(&mut complete_opts);
        let barrier = MultipartCommitBarrier::install(RUSTFS_META_BUCKET, object, MultipartCommitPause::BeforeQuotaRename);
        let complete_store = Arc::clone(&lossy_store);
        let complete_upload_id = upload.upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload_for_data_movement(
                    (2, None),
                    RUSTFS_META_BUCKET,
                    object,
                    &complete_upload_id,
                    completed_parts,
                    &complete_opts,
                )
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the data-movement capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), complete)
            .await
            .expect("data-movement CompleteMultipartUpload should finish after the commit barrier release")
            .expect("data-movement CompleteMultipartUpload task should not panic")
            .expect_err("a lost data-movement capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "data-movement lease loss should remain observable");
        drop(barrier);

        let object_err = lossy_store.pools[2]
            .get_object_info(
                RUSTFS_META_BUCKET,
                object,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("lost data-movement capacity lease must not publish the object");
        assert!(
            crate::error::is_err_object_not_found(&object_err),
            "data-movement lease loss should leave the target object absent: {object_err}"
        );
        lossy_store.pools[2]
            .get_multipart_info(
                RUSTFS_META_BUCKET,
                object,
                &upload.upload_id,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("data-movement lease loss must leave the staged MPU retryable");
        let listed = lossy_store.pools[2]
            .list_object_parts(
                RUSTFS_META_BUCKET,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("data-movement lease loss should preserve the staged part");
        assert_eq!(listed.parts.len(), 1, "data-movement lease loss must preserve the MPU part");
        let meta = lossy_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement reservation should remain durable after the fenced failure");
        assert_eq!(
            reservation.consumed_target_physical_bytes, 0,
            "a pre-rename data-movement failure must not consume durable target capacity"
        );
        drop(meta);
        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the fenced data-movement failure should leave readable pool metadata");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the fenced data-movement reservation should remain persisted");
        assert_eq!(
            persisted_reservation.consumed_target_physical_bytes, 0,
            "the pre-rename data-movement failure must not persist consumed target capacity"
        );
    }

    async fn assert_data_movement_complete_waits_for_capacity_tail() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let object = format!("data-movement-complete-tail-{}.bin", uuid::Uuid::new_v4());
        let body = vec![0x5a; 64];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 4096, 4096),
                DecommissionPoolCapacityInfo::for_test(2, layout, 4096, 4096, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the data-movement tail capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let reserved_target = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement tail reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(reserved_target, 2, "the data-movement tail target must be pool 2");

        let upload = new_multipart_upload(
            &store,
            2,
            RUSTFS_META_BUCKET,
            &object,
            ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                ..Default::default()
            },
        )
        .await
        .expect("create the staged data-movement tail upload");
        let multipart_opts = multipart_options(
            &store,
            RUSTFS_META_BUCKET,
            ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                ..Default::default()
            },
        )
        .await;
        let mut data = PutObjReader::from_vec(body.clone());
        let part = store.pools[2]
            .put_object_part(RUSTFS_META_BUCKET, &object, &upload.upload_id, 1, &mut data, &multipart_opts)
            .await
            .expect("stage the data-movement tail part");
        let completed_parts = vec![crate::storage_api_contracts::multipart::CompletePart {
            part_num: part.part_num,
            etag: part.etag,
            ..Default::default()
        }];

        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let mut complete_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            http_preconditions: Some(data_movement::data_movement_target_precondition()),
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        owner.apply_to(&mut complete_opts);

        let tail_barrier =
            crate::set_disk::rename_fanout_barrier::arm(&object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
        let complete_store = Arc::clone(&other_store);
        let complete_object = object.clone();
        let complete_upload_id = upload.upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload_for_data_movement(
                    (2, None),
                    RUSTFS_META_BUCKET,
                    &complete_object,
                    &complete_upload_id,
                    completed_parts,
                    &complete_opts,
                )
                .await
        });
        tail_barrier.wait_until_paused().await;

        {
            let meta = other_store.pool_meta.read().await;
            let reservation = meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("paused data-movement completion reservation should remain present");
            assert!(
                reservation.pending_target_physical_bytes > 0 || reservation.inflight_target_physical_bytes > 0,
                "paused data-movement completion must retain pending or inflight capacity"
            );
            assert_eq!(
                reservation.consumed_target_physical_bytes, 0,
                "paused data-movement completion must not persist consumed capacity"
            );
        }

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{reserved_target}"),
            )
            .await
            .expect("create the data-movement tail target-fence probe");
        let mut target_probe = tokio::spawn(async move { target_lock.get_write_lock(std::time::Duration::from_secs(30)).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut target_probe)
                .await
                .is_err(),
            "data-movement completion must retain its target fence while the tail is paused"
        );
        target_probe.abort();
        let _ = target_probe.await;

        let pool_meta_lock = other_store
            .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
            .await
            .expect("create the data-movement tail pool metadata probe");
        let pool_meta_guard = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool_meta_lock.get_write_lock(std::time::Duration::from_secs(30)),
        )
        .await
        .expect("per-target data movement must leave pool metadata available during target I/O")
        .expect("pool metadata probe should acquire during the target tail");
        drop(pool_meta_guard);

        tail_barrier.release();
        drop(tail_barrier);
        tokio::time::timeout(std::time::Duration::from_secs(30), complete)
            .await
            .expect("data-movement CompleteMultipartUpload should finish after its tail resumes")
            .expect("data-movement CompleteMultipartUpload task should not panic")
            .expect("data-movement CompleteMultipartUpload should commit after its tail resumes");
        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{reserved_target}"),
            )
            .await
            .expect("create the post-tail target-fence probe");
        let target_guard = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            target_lock.get_write_lock(std::time::Duration::from_secs(30)),
        )
        .await
        .expect("target-fence probe should finish after data-movement tail completion")
        .expect("target-fence probe should acquire after data-movement tail completion");
        drop(target_guard);

        let meta = other_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("completed data-movement tail reservation should remain present");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert!(
            reservation.consumed_target_physical_bytes > 0,
            "data-movement completion should consume capacity only after its tail"
        );
        drop(meta);

        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("data-movement tail completion should persist readable pool metadata");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("persisted data-movement tail reservation should remain present");
        assert_eq!(persisted_reservation.pending_target_physical_bytes, 0);
        assert_eq!(persisted_reservation.inflight_target_physical_bytes, 0);
        assert!(persisted_reservation.consumed_target_physical_bytes > 0);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_complete_does_not_release_capacity_before_early_ack_tail() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            assert_data_movement_complete_waits_for_capacity_tail().await
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_put_fences_capacity_lease_loss_before_publication() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            assert_data_movement_put_fences_capacity_lease_loss_before_publication().await
        })
        .await;
    }

    async fn assert_data_movement_put_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let object = "data-movement-put-lease-loss.bin";
        let body = vec![0x52; 256 * 1024];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the data-movement PUT reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let reservation_target = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement PUT reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(reservation_target, 2, "data-movement PUT should use the reserved target");

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let version_id = uuid::Uuid::new_v4().to_string();
        let mut put_opts = ObjectOptions {
            data_movement: true,
            versioned: true,
            version_id: Some(version_id.clone()),
            src_pool_idx: 0,
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        owner.apply_to(&mut put_opts);
        let barrier = PutObjectCommitBarrier::install(RUSTFS_META_BUCKET, object, PutObjectCommitPause::BeforeQuotaRename);
        let put_store = Arc::clone(&lossy_store);
        let put_opts = put_opts.clone();
        let put_body = body.clone();
        let put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(put_body);
            put_store
                .put_object_for_data_movement(RUSTFS_META_BUCKET, object, &mut data, &put_opts, None)
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the data-movement PUT capacity lease must lose refresh quorum"
        );
        barrier.release();
        tokio::time::resume();
        let (_target_idx, result) = tokio::time::timeout(Duration::from_secs(2), put)
            .await
            .expect("data-movement PUT should finish after the commit barrier release")
            .expect("data-movement PUT task should not panic")
            .expect("data-movement PUT should select its reserved target");
        let err = result.expect_err("a lost data-movement PUT capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "data-movement PUT lease loss should remain observable");
        drop(barrier);

        let object_err = lossy_store.pools[2]
            .get_object_info(
                RUSTFS_META_BUCKET,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("lost data-movement PUT capacity lease must not publish the object");
        assert!(
            matches!(&object_err, crate::error::Error::VersionNotFound(b, o, v)
                if b == RUSTFS_META_BUCKET && o == object && v == &version_id),
            "data-movement PUT lease loss should leave the target object absent: {object_err}"
        );

        let meta = lossy_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement PUT reservation should remain after the fenced failure");
        assert!(
            reservation.pending_target_physical_bytes > 0,
            "failed data-movement PUT must retain its durable pending reservation"
        );
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reservation.consumed_target_physical_bytes, 0);
        let live_progress = (
            reservation.pending_target_physical_bytes,
            reservation.inflight_target_physical_bytes,
            reservation.consumed_target_physical_bytes,
            reservation.observed_target_physical_bytes,
            reservation.committed_data_bytes,
        );
        drop(meta);

        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the fenced data-movement PUT failure should leave readable pool metadata");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the fenced data-movement PUT reservation should remain persisted");
        assert_eq!(
            (
                persisted_reservation.pending_target_physical_bytes,
                persisted_reservation.inflight_target_physical_bytes,
                persisted_reservation.consumed_target_physical_bytes,
                persisted_reservation.observed_target_physical_bytes,
                persisted_reservation.committed_data_bytes,
            ),
            live_progress,
            "fenced data-movement PUT progress must match its durable replica"
        );

        *other_store.pool_meta.write().await = persisted.clone();
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
            ],
        );
        let retry_owner = decommission_capacity_owner(&persisted);
        let mut retry_opts = ObjectOptions {
            data_movement: true,
            versioned: true,
            version_id: Some(version_id.clone()),
            src_pool_idx: 0,
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        retry_owner.apply_to(&mut retry_opts);
        let mut retry_data = PutObjReader::from_vec(body.clone());
        let (retry_target, retry_result) = other_store
            .put_object_for_data_movement(RUSTFS_META_BUCKET, object, &mut retry_data, &retry_opts, None)
            .await
            .expect("same-mutation retry should reach the staged target write");
        assert_eq!(retry_target, 2);
        retry_result.expect("same-mutation retry should recover the pending intent and publish");
        let retry_info = other_store.pools[2]
            .get_object_info(
                RUSTFS_META_BUCKET,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id),
                    ..Default::default()
                },
            )
            .await
            .expect("same-mutation retry should publish a readable target");
        assert_eq!(retry_info.size, i64::try_from(body.len()).expect("test body size fits i64"));

        let next_object = "data-movement-put-after-retry.bin";
        let next_version_id = uuid::Uuid::new_v4().to_string();
        let mut next_opts = ObjectOptions {
            data_movement: true,
            versioned: true,
            version_id: Some(next_version_id.clone()),
            src_pool_idx: 0,
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        retry_owner.apply_to(&mut next_opts);
        let mut next_data = PutObjReader::from_vec(body);
        let next_result = other_store
            .put_object_for_data_movement(RUSTFS_META_BUCKET, next_object, &mut next_data, &next_opts, None)
            .await;
        assert!(next_result.is_err(), "a different mutation must not reuse A's recovered capacity");
        let next_target_err = other_store.pools[2]
            .get_object_info(
                RUSTFS_META_BUCKET,
                next_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_version_id.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the next mutation must remain unpublished after A recovery");
        assert!(
            matches!(&next_target_err, crate::error::Error::VersionNotFound(b, o, v)
                if b == RUSTFS_META_BUCKET && o == next_object && v == &next_version_id),
            "the next mutation must leave its exact target version absent: {next_target_err}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_put_holds_capacity_through_successful_early_ack_tail() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            assert_data_movement_put_holds_capacity_through_successful_tail().await
        })
        .await;
    }

    async fn assert_data_movement_put_holds_capacity_through_successful_tail() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let object = "data-movement-put-success-tail.bin";
        let body = vec![0x53; 256 * 1024];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the successful data-movement PUT reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let reservation_target = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("successful data-movement PUT reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(reservation_target, 2, "successful data-movement PUT should use the reserved target");

        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let version_id = uuid::Uuid::new_v4().to_string();
        let mut put_opts = ObjectOptions {
            data_movement: true,
            versioned: true,
            version_id: Some(version_id.clone()),
            src_pool_idx: 0,
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        owner.apply_to(&mut put_opts);
        let tail_barrier =
            crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
        let put_store = Arc::clone(&other_store);
        let put_opts = put_opts.clone();
        let put_body = body.clone();
        let mut put = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(put_body);
            put_store
                .put_object_for_data_movement(RUSTFS_META_BUCKET, object, &mut data, &put_opts, None)
                .await
        });
        tail_barrier.wait_until_paused().await;

        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut put).await.is_err(),
            "owner-bearing data-movement PUT must wait for the rename tail instead of early-ACKing"
        );
        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{reservation_target}"),
            )
            .await
            .expect("create the data-movement PUT tail target-fence probe");
        let mut target_probe = tokio::spawn(async move { target_lock.get_write_lock(Duration::from_secs(30)).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut target_probe)
                .await
                .is_err(),
            "data-movement PUT tail must retain its target fence"
        );
        target_probe.abort();
        let _ = target_probe.await;

        let pool_meta_lock = other_store
            .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
            .await
            .expect("create the data-movement PUT tail pool metadata probe");
        let pool_meta_guard =
            tokio::time::timeout(Duration::from_secs(1), pool_meta_lock.get_write_lock(Duration::from_secs(30)))
                .await
                .expect("per-target data movement must leave pool metadata available during target I/O")
                .expect("pool metadata probe should acquire during the target tail");
        drop(pool_meta_guard);
        {
            let meta = other_store.pool_meta.read().await;
            let reservation = meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("paused successful data-movement PUT reservation should remain present");
            assert!(
                reservation.pending_target_physical_bytes > 0,
                "paused successful data-movement PUT must retain pending capacity"
            );
            assert_eq!(reservation.inflight_target_physical_bytes, 0);
            assert_eq!(reservation.consumed_target_physical_bytes, 0);
        }

        tail_barrier.release();
        drop(tail_barrier);
        let (target_pool_idx, put_result) = tokio::time::timeout(Duration::from_secs(30), put)
            .await
            .expect("data-movement PUT should finish after its rename tail resumes")
            .expect("data-movement PUT task should not panic")
            .expect("data-movement PUT should select its reserved target");
        let committed = put_result.expect("data-movement PUT should commit after its rename tail resumes");
        assert_eq!(target_pool_idx, 2);
        assert_eq!(committed.version_id.map(|id| id.to_string()), Some(version_id.clone()));
        let target = other_store.pools[2]
            .get_object_info(
                RUSTFS_META_BUCKET,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id),
                    ..Default::default()
                },
            )
            .await
            .expect("successful data-movement PUT should remain readable after its tail");
        assert!(!target.delete_marker);

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{reservation_target}"),
            )
            .await
            .expect("create the post-tail data-movement target-fence probe");
        let target_guard = tokio::time::timeout(Duration::from_secs(30), target_lock.get_write_lock(Duration::from_secs(30)))
            .await
            .expect("target-fence probe should finish after the data-movement PUT tail")
            .expect("target-fence probe should acquire after the data-movement PUT tail");
        drop(target_guard);

        let meta = other_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("completed data-movement PUT reservation should remain present");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert!(
            reservation.consumed_target_physical_bytes > 0,
            "data-movement PUT should consume capacity only after its tail"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn target_gate_retry_reloads_exact_source_identity_before_copying() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("target-gate-source-change");
        let object = "source-changes-while-target-busy.bin";
        let initial_body = vec![0x41; 64 * 1024];
        let replacement_body = vec![0x42; initial_body.len()];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the target-gate source-change bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the target-gate source-change bucket incarnation");
        let initial_mod_time = time::OffsetDateTime::now_utc();
        let mut initial_data = PutObjReader::from_vec(initial_body);
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut initial_data,
                &ObjectOptions {
                    mod_time: Some(initial_mod_time),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the initial source identity");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = replacement_body.len().saturating_mul(4);
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, replacement_body.len(), replacement_body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]],
        );
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the target-gate source-change reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, replacement_body.len(), replacement_body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]],
        );
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| reservation.targets.first())
            .expect("the source-change reservation should allocate a target")
            .pool_index;
        let in_memory_reservation = other_store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the source-change reservation should be published in memory")
            .clone();
        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("the source-change reservation should reload from durable replicas");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the durable source-change reservation should remain present");
        assert_eq!(
            (
                persisted_reservation.operation_id,
                persisted_reservation.generation,
                persisted_reservation.owner_nonce,
                persisted_reservation.model_version,
                persisted_reservation.targets.clone(),
            ),
            (
                in_memory_reservation.operation_id,
                in_memory_reservation.generation,
                in_memory_reservation.owner_nonce,
                in_memory_reservation.model_version,
                in_memory_reservation.targets.clone(),
            ),
            "the worker owner identity must match the durable reservation"
        );
        assert!(
            persisted_reservation.expires_at > time::OffsetDateTime::now_utc(),
            "the durable reservation lease must remain active before the worker starts"
        );

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the target-gate source-change lock");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the target gate before starting migration");
        let retry_observer = DecommissionCapacityLockOrderBarrier::install(other_store.id, other_store.id);
        let source_set = other_store.pools[0].get_disks_by_key(object);

        let cancel_token = CancellationToken::new();
        let canceled_store = Arc::clone(&other_store);
        let canceled_bucket = bucket.clone();
        let canceled_set = Arc::clone(&source_set);
        let canceled_token = cancel_token.clone();
        let mut canceled_worker = tokio::spawn(async move {
            canceled_store
                .decommission_entry_with_retry_state_for_test(
                    canceled_token,
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    canceled_bucket,
                    canceled_set,
                    None,
                    Arc::new(AtomicUsize::new(0)),
                )
                .await
        });
        tokio::select! {
            _ = retry_observer.wait_until_target_gate_retry() => {}
            result = &mut canceled_worker => {
                panic!("cancellation probe finished before observing target contention: {result:?}");
            }
        }
        // The mutation absorbs the gate contention inline before the outer wait
        // loop ever sees it, and that inline budget is bounded.
        assert_eq!(
            retry_observer.target_gate_inline_retries(),
            crate::core::pools::DECOMMISSION_MUTATION_GATE_MAX_INLINE_ATTEMPTS - 1,
            "inline target-gate retries must stop at their bounded attempt budget"
        );
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            0,
            "a long-held target gate must not trigger periodic exact quorum reloads"
        );
        cancel_token.cancel();
        let canceled_err = tokio::time::timeout(Duration::from_secs(2), canceled_worker)
            .await
            .expect("target-gate wait should observe cancellation without waiting for release")
            .expect("cancellation probe task should not panic")
            .expect_err("the canceled target-gate wait must stop the entry");
        assert!(
            crate::error::is_err_operation_canceled(&canceled_err),
            "unexpected target-gate cancellation error: {canceled_err:?}"
        );

        let worker_store = Arc::clone(&other_store);
        let worker_bucket = bucket.clone();
        let mut worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });
        tokio::select! {
            _ = retry_observer.wait_until_target_gate_retry() => {}
            result = &mut worker => {
                panic!("decommission entry finished before observing target contention: {result:?}");
            }
        }

        let mut replacement_data = PutObjReader::from_vec(replacement_body.clone());
        other_store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut replacement_data,
                &ObjectOptions {
                    mod_time: Some(initial_mod_time + time::Duration::seconds(1)),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("replace the source after the target-busy attempt releases its source snapshot");
        retry_observer.release_owner();
        drop(target_guard);
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("source-change retry should finish after releasing the target gate")
            .expect("source-change retry task should not panic")
            .expect("source-change retry should converge on the replacement identity");
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            1,
            "source identity should be reloaded exactly once after the target gate becomes available"
        );
        drop(retry_observer);

        let mut target_reader = other_store.pools[target_pool_index]
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("replacement source should be migrated to the reserved target");
        let mut target_body = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut target_body)
            .await
            .expect("read the migrated replacement body");
        assert_eq!(target_body, replacement_body, "the stale pre-contention snapshot must never be copied");
        let source_err = other_store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("the converged replacement source should be removed after migration");
        assert!(
            matches!(source_err, crate::error::Error::ObjectNotFound(_, _)),
            "unexpected source state after replacement migration: {source_err:?}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn target_gate_retry_pins_or_releases_permits_when_target_rank_changes() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("target-gate-two-waiters");
        let objects = ["first-waiter.bin", "second-waiter.bin"];
        let body = vec![0x54; 64 * 1024];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the two-waiter target-gate bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the two-waiter bucket incarnation");
        for object in objects {
            let mut source_data = PutObjReader::from_vec(body.clone());
            store.pools[0]
                .put_object(
                    &bucket,
                    object,
                    &mut source_data,
                    &ObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed a same-target waiter source");
        }

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source_bytes = body.len().saturating_mul(4);
        let first_target_capacity = body.len().saturating_mul(5);
        let second_target_capacity = body.len().saturating_mul(3);
        let capacity_snapshot = vec![
            DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_bytes, source_bytes),
            DecommissionPoolCapacityInfo::for_test(1, layout, first_target_capacity, first_target_capacity, 0),
            DecommissionPoolCapacityInfo::for_test(2, layout, second_target_capacity, second_target_capacity, 0),
        ];
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot.clone()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the two-waiter capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        set_decommission_capacity_info_overrides_for_test(other_store.id, vec![capacity_snapshot]);
        let capacity_owner = {
            let pool_meta = other_store.pool_meta.read().await;
            decommission_capacity_owner(&pool_meta)
        };
        let target_pool_index = other_store
            .select_decommission_capacity_target_pool(capacity_owner, body.len())
            .await
            .expect("select the initially largest target allocation");
        let target_pool_indices = other_store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the two-waiter reservation should remain active")
            .targets
            .iter()
            .map(|target| target.pool_index)
            .collect::<Vec<_>>();
        assert_eq!(target_pool_indices.len(), 2, "the rank-change fixture needs two eligible targets");
        let alternate_target_pool_index = target_pool_indices
            .iter()
            .copied()
            .find(|pool_index| *pool_index != target_pool_index)
            .expect("the rank-change fixture should have an alternate target");

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the two-waiter target gate");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the target gate before starting both waiters");
        let retry_observer = DecommissionCapacityLockOrderBarrier::install(other_store.id, other_store.id);

        let mut workers = Vec::new();
        for object in objects {
            let worker_store = Arc::clone(&other_store);
            let worker_bucket = bucket.clone();
            let source_set = other_store.pools[0].get_disks_by_key(object);
            workers.push(tokio::spawn(async move {
                worker_store
                    .decommission_entry_for_test(
                        0,
                        MetaCacheEntry {
                            name: object.to_string(),
                            ..Default::default()
                        },
                        worker_bucket,
                        source_set,
                    )
                    .await
            }));
        }
        retry_observer.wait_until_target_gate_retries(2).await;
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            0,
            "waiters must not poll exact source metadata while the target gate is held"
        );

        let holder_consumed = body.len().saturating_add(1);
        {
            let mut pool_meta = other_store.pool_meta.write().await;
            let source_pool = &mut pool_meta.pools[0];
            let reservation = source_pool
                .decommission
                .as_mut()
                .and_then(|info| info.capacity_reservation.as_mut())
                .expect("the holder commit should update the active reservation");
            let target = reservation
                .targets
                .iter_mut()
                .find(|target| target.pool_index == target_pool_index)
                .expect("the holder commit target should remain allocated");
            target.consumed_physical_bytes = target.consumed_physical_bytes.saturating_add(holder_consumed);
            target.observed_physical_bytes = target.observed_physical_bytes.saturating_add(holder_consumed);
            reservation.committed_data_bytes = reservation.committed_data_bytes.saturating_add(holder_consumed);
            reservation.consumed_target_physical_bytes =
                reservation.consumed_target_physical_bytes.saturating_add(holder_consumed);
            reservation.observed_target_physical_bytes =
                reservation.observed_target_physical_bytes.saturating_add(holder_consumed);
            reservation.prediction_error_bytes = 0;
            source_pool.last_update = time::OffsetDateTime::now_utc();
        }
        other_store
            .save_current_pool_meta_for_test(&[0])
            .await
            .expect("persist the holder commit while it owns the original target gate");
        assert_eq!(
            other_store
                .select_decommission_capacity_target_pool(capacity_owner, body.len())
                .await
                .expect("reselect after the holder commit"),
            alternate_target_pool_index,
            "the holder commit should flip the dynamic target ranking"
        );

        drop(target_guard);
        retry_observer.wait_until_owner_paused().await;
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            1,
            "the first waiter must retain its original target despite the changed ranking"
        );
        let alternate_probe = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{alternate_target_pool_index}"),
            )
            .await
            .expect("create the alternate target-gate probe")
            .get_write_lock_quiet(Duration::from_millis(300))
            .await
            .expect("a pinned waiter must not hold the alternate target gate");
        drop(alternate_probe);
        retry_observer.release_owner();
        retry_observer.wait_until_owner_paused().await;
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            2,
            "the second waiter should reload exactly once before releasing its exhausted original permit"
        );
        let original_probe = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the original target-gate probe")
            .get_write_lock_quiet(Duration::from_millis(300))
            .await
            .expect("a reselected waiter must release its original target gate before taking the alternate");
        drop(original_probe);
        retry_observer.release_owner();

        for worker in workers {
            tokio::time::timeout(Duration::from_secs(30), worker)
                .await
                .expect("same-target waiter should finish after permit transfer")
                .expect("same-target waiter task should not panic")
                .expect("same-target waiter migration should succeed");
        }
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            2,
            "formal mutations must consume their permits without re-entering target-busy recovery"
        );
        drop(retry_observer);

        for object in objects {
            let mut target_copies = 0;
            for pool_index in &target_pool_indices {
                if other_store.pools[*pool_index]
                    .get_object_info(&bucket, object, &ObjectOptions::default())
                    .await
                    .is_ok()
                {
                    target_copies += 1;
                }
            }
            assert_eq!(target_copies, 1, "each waiter should publish on exactly one reserved target");
            let source_err = other_store.pools[0]
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect_err("each migrated waiter source should be removed");
            assert!(matches!(source_err, crate::error::Error::ObjectNotFound(_, _)));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn later_version_target_busy_does_not_replay_an_earlier_version() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("target-gate-later-version");
        let object = "two-versions-one-busy.bin";
        let bodies = [vec![0x31; 64 * 1024], vec![0x32; 64 * 1024]];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the later-version target-gate bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the later-version bucket incarnation");
        for body in &bodies {
            let mut source_data = PutObjReader::from_vec(body.clone());
            store.pools[0]
                .put_object(
                    &bucket,
                    object,
                    &mut source_data,
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(uuid::Uuid::new_v4().to_string()),
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed a source version");
        }

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source_bytes = bodies.iter().map(Vec::len).sum::<usize>();
        let target_total = source_bytes.saturating_mul(4);
        let capacity_snapshot = vec![
            DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_bytes, source_bytes),
            DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
            DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
        ];
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot.clone()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the later-version capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        set_decommission_capacity_info_overrides_for_test(other_store.id, vec![capacity_snapshot]);
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| reservation.targets.first())
            .expect("the later-version reservation should allocate a target")
            .pool_index;

        let successful_copies = Arc::new(AtomicUsize::new(0));
        let first_copy_ready = Arc::new(tokio::sync::Notify::new());
        let first_copy_release = Arc::new((StdMutex::new(false), Condvar::new()));
        let hook_bucket = bucket.clone();
        let hook_object = object.to_string();
        let hook_copies = Arc::clone(&successful_copies);
        let hook_ready = Arc::clone(&first_copy_ready);
        let hook_release = Arc::clone(&first_copy_release);
        let fault_guard = DecommissionTestFaultGuard::install(Arc::new(move |stage, bucket, object, _attempt, succeeded| {
            if succeeded && stage == "migrate_object" && bucket == hook_bucket && object == hook_object {
                let copy_number = hook_copies.fetch_add(1, Ordering::AcqRel) + 1;
                if copy_number == 1 {
                    hook_ready.notify_one();
                    let (released, wake) = &*hook_release;
                    let mut released = released.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    while !*released {
                        released = wake.wait(released).unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                }
            }
            false
        }));

        let worker_store = Arc::clone(&other_store);
        let worker_bucket = bucket.clone();
        let source_set = other_store.pools[0].get_disks_by_key(object);
        let mut worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), first_copy_ready.notified())
            .await
            .expect("the first version should complete its target copy");

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the later-version target gate");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the target gate after the first version copy");
        let retry_observer = DecommissionCapacityLockOrderBarrier::install(other_store.id, other_store.id);
        {
            let (released, wake) = &*first_copy_release;
            *released.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            wake.notify_one();
        }
        tokio::select! {
            _ = retry_observer.wait_until_target_gate_retry() => {}
            result = &mut worker => panic!("later-version worker finished before target contention: {result:?}"),
        }
        assert_eq!(
            successful_copies.load(Ordering::Acquire),
            1,
            "the already copied version must not be replayed while the later version waits"
        );

        retry_observer.release_owner();
        drop(target_guard);
        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("later-version worker should finish after target release")
            .expect("later-version worker task should not panic")
            .expect("later-version migration should succeed");
        assert_eq!(
            successful_copies.load(Ordering::Acquire),
            2,
            "each source version should complete exactly one target copy"
        );
        drop(retry_observer);
        drop(fault_guard);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn target_busy_does_not_reset_the_ordinary_copy_failure_budget() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("target-gate-copy-budget");
        let object = "copy-fails-around-target-busy.bin";
        let body = vec![0x43; 64 * 1024];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the copy-budget target-gate bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the copy-budget bucket incarnation");
        let mut source_data = PutObjReader::from_vec(body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_data,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the copy-budget source");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let source_bytes = body.len().saturating_mul(3);
        let target_total = body.len().saturating_mul(8);
        let capacity_snapshot = vec![
            DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_bytes, source_bytes),
            DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
            DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
        ];
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot.clone()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the copy-budget capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        set_decommission_capacity_info_overrides_for_test(other_store.id, vec![capacity_snapshot]);
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| reservation.targets.first())
            .expect("the copy-budget reservation should allocate a target")
            .pool_index;

        let observed_attempts = Arc::new(StdMutex::new(Vec::new()));
        let fault_ready = Arc::new(tokio::sync::Notify::new());
        let fault_release = Arc::new((StdMutex::new(0usize), Condvar::new()));
        let hook_bucket = bucket.clone();
        let hook_object = object.to_string();
        let hook_attempts = Arc::clone(&observed_attempts);
        let hook_ready = Arc::clone(&fault_ready);
        let hook_release = Arc::clone(&fault_release);
        let fault_guard = DecommissionTestFaultGuard::install(Arc::new(move |stage, bucket, object, attempt, succeeded| {
            if stage != "migrate_object" || bucket != hook_bucket || object != hook_object {
                return false;
            }
            let copy_number = {
                let mut attempts = hook_attempts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                attempts.push(attempt);
                attempts.len()
            };
            if copy_number <= 2 {
                hook_ready.notify_one();
                let (released, wake) = &*hook_release;
                let mut released = released.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                while *released < copy_number {
                    released = wake.wait(released).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                return succeeded;
            }
            false
        }));

        let worker_store = Arc::clone(&other_store);
        let worker_bucket = bucket.clone();
        let source_set = other_store.pools[0].get_disks_by_key(object);
        let mut worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), fault_ready.notified())
            .await
            .expect("the first successful copy should reach fault injection");

        let target_lock = other_store
            .new_ns_lock(
                RUSTFS_META_BUCKET,
                &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/{target_pool_index}"),
            )
            .await
            .expect("create the copy-budget target gate");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the target gate after the first injected copy failure");
        let retry_observer = DecommissionCapacityLockOrderBarrier::install(other_store.id, other_store.id);
        retry_observer.disable_owner_pause();
        {
            let (released, wake) = &*fault_release;
            *released.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = 1;
            wake.notify_all();
        }
        tokio::select! {
            _ = retry_observer.wait_until_target_gate_retry() => {}
            result = &mut worker => panic!("copy-budget worker finished before target contention: {result:?}"),
        }
        drop(target_guard);
        tokio::time::timeout(Duration::from_secs(30), fault_ready.notified())
            .await
            .expect("the post-contention copy should reach the second injected failure");
        assert_eq!(
            *observed_attempts.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![1, 2],
            "target contention must not reset the attempt consumed before it"
        );
        drop(retry_observer);
        {
            let (released, wake) = &*fault_release;
            *released.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = 2;
            wake.notify_all();
        }

        tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .expect("copy-budget worker should finish on its final attempt")
            .expect("copy-budget worker task should not panic")
            .expect("copy-budget migration should succeed within its original retry budget");
        assert_eq!(
            *observed_attempts.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![1, 2, 3],
            "ordinary copy failures before and after contention must consume one shared budget"
        );
        drop(fault_guard);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn exact_delete_reconciles_pending_capacity_before_removing_replicas() {
        let (_temp_dirs, store, _other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("exact-delete-capacity");
        let object = "published-before-exact-delete.bin";
        let body = vec![0x55; 64 * 1024];
        let version_id = uuid::Uuid::new_v4().to_string();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the exact-delete reconciliation bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the exact-delete bucket incarnation");

        let mut source_data = PutObjReader::from_vec(body.clone());
        let source = store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_data,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.clone()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the exact source version");
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]],
        );
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the exact-delete capacity reservation");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let target_pool_index = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the exact-delete capacity reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(target_pool_index, 2);

        let target_options = ObjectOptions {
            versioned: true,
            version_id: Some(version_id.clone()),
            mod_time: source.mod_time,
            preserve_etag: source.etag.clone(),
            user_defined: (*source.user_defined).clone(),
            data_movement: true,
            src_pool_idx: 0,
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let mut target_data = PutObjReader::from_vec(body.clone());
        let target = store.pools[target_pool_index]
            .put_object(&bucket, object, &mut target_data, &target_options)
            .await
            .expect("publish the target version before capacity progress");
        assert_eq!(target.version_id, source.version_id);
        assert_eq!(target.mod_time, source.mod_time);
        assert_eq!(target.size, source.size);

        let source_version_id = source.version_id.map(|version_id| version_id.to_string());
        let mutation_id = decommission_capacity_mutation_id(
            owner,
            &source.bucket,
            &source.name,
            source_version_id.as_deref(),
            source.delete_marker,
            source.mod_time,
        );
        {
            let mut pool_meta = store.pool_meta.write().await;
            let source_pool = &mut pool_meta.pools[0];
            let reservation = source_pool
                .decommission
                .as_mut()
                .and_then(|info| info.capacity_reservation.as_mut())
                .expect("the exact-delete capacity reservation should remain active");
            let target = reservation
                .targets
                .iter_mut()
                .find(|target| target.pool_index == target_pool_index)
                .expect("the exact-delete target allocation should exist");
            target.pending_physical_bytes = body.len();
            target.pending_mutation_id = Some(mutation_id);
            reservation.pending_target_physical_bytes = body.len();
            source_pool.last_update = time::OffsetDateTime::now_utc();
        }
        store
            .save_current_pool_meta_for_test(&[0])
            .await
            .expect("persist the simulated post-commit capacity intent");

        let exact_delete_options = ObjectOptions {
            versioned: true,
            version_id: Some(version_id.clone()),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        store.pools[target_pool_index]
            .delete_object(&bucket, object, exact_delete_options.clone())
            .await
            .expect("remove the target evidence before the fail-closed exact delete");
        let delete_err = store
            .delete_object(&bucket, object, exact_delete_options.clone())
            .await
            .expect_err("exact delete must fail while its pending target evidence is absent");
        assert!(matches!(delete_err, crate::error::Error::DecommissionCapacityBlocked { .. }));
        store.pools[0]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.clone()),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("a failed reconciliation must preserve the source evidence");
        let mut failed = crate::core::pools::PoolMeta::default();
        failed
            .load_no_lock_from_replicas(store.pools.clone())
            .await
            .expect("the failed exact delete must preserve readable capacity metadata");
        let failed_reservation = failed.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the failed exact delete reservation should remain present");
        assert_eq!(failed_reservation.pending_target_physical_bytes, body.len());
        assert_eq!(failed_reservation.consumed_target_physical_bytes, 0);

        let mut replacement_target_data = PutObjReader::from_vec(body.clone());
        store.pools[target_pool_index]
            .put_object(&bucket, object, &mut replacement_target_data, &target_options)
            .await
            .expect("restore the equivalent target evidence for the exact-delete retry");

        store
            .delete_object(&bucket, object, exact_delete_options)
            .await
            .expect("the exact delete should reconcile capacity before removing replicas");

        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(store.pools.clone())
            .await
            .expect("the exact-delete reconciliation should remain durable");
        let reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the reconciled reservation should remain present");
        assert_eq!(reservation.pending_target_physical_bytes, 0);
        assert_eq!(reservation.consumed_target_physical_bytes, body.len());
        assert_eq!(reservation.committed_data_bytes, body.len());

        for pool_index in [0, target_pool_index] {
            let err = store.pools[pool_index]
                .get_object_info(
                    &bucket,
                    object,
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(version_id.clone()),
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect_err("the exact version should be absent after reconciliation and delete");
            assert!(crate::error::is_err_object_not_found(&err) || crate::error::is_err_version_not_found(&err));
        }
    }

    #[test]
    #[serial_test::serial]
    fn scanner_backlog_cas_keeps_fences_after_waiter_cancellation_until_rename_drains() {
        run_large_stack_current_thread_async_test("scanner-backlog-canceled-waiter", async || {
            temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
                let (_temp_dirs, writer, other) =
                    test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
                let object = "buckets/.scanner-pause-backlog.json";
                let set_index = 1;
                let body = vec![0x37; 1024];
                assert!(
                    !writer.pools[0].disk_set[0]
                        .shares_namespace_lock_domain(&writer.pools[0].disk_set[set_index])
                        .await
                );
                let rename_tasks = crate::set_disk::rename_fanout_barrier::observe_tasks(object);
                let tail =
                    crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
                let put_store = Arc::clone(&writer);
                let put_body = body.clone();
                let mut put = tokio::spawn(async move {
                    put_store
                        .save_scanner_pause_backlog_replica(0, set_index, put_body, Default::default())
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), tail.wait_until_paused())
                    .await
                    .expect("the native write must reach its held rename");
                tokio::time::timeout(Duration::from_secs(30), async {
                    while rename_tasks.running() != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the other disks must reach quorum before canceling the waiter");
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), &mut put).await.is_err(),
                    "native replica publication must await the entire rename tail"
                );
                put.abort();
                assert!(put.await.expect_err("the scanner waiter must be canceled").is_cancelled());

                let capacity_lock = other
                    .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
                    .await
                    .expect("capacity lock probe");
                let object_lock = other
                    .new_ns_lock(RUSTFS_META_BUCKET, object)
                    .await
                    .expect("fixed object lock probe");
                let mut capacity_probe = tokio::spawn(async move { capacity_lock.get_write_lock(Duration::from_secs(30)).await });
                let mut object_probe = tokio::spawn(async move { object_lock.get_write_lock(Duration::from_secs(30)).await });
                for (label, probe) in [("capacity", &mut capacity_probe), ("fixed object", &mut object_probe)] {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(100), probe).await.is_err(),
                        "canceling the scanner waiter must retain its {label} fence while rename is pending"
                    );
                }
                tail.release();
                drop(tail);
                for probe in [capacity_probe, object_probe] {
                    drop(
                        tokio::time::timeout(Duration::from_secs(30), probe)
                            .await
                            .expect("publication fence must drain after rename")
                            .expect("lock probe must not panic")
                            .expect("publication fence must eventually be released"),
                    );
                }
                let mut reader = writer.pools[0].disk_set[set_index]
                    .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("canceled waiter must leave the committed replica readable");
                let mut actual = Vec::new();
                reader
                    .read_to_end(&mut actual)
                    .await
                    .expect("read the full native replica after tail drain");
                assert_eq!(actual, body);
            })
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn scanner_backlog_cas_rejects_lost_capacity_lease_before_publication() {
        run_large_stack_current_thread_async_test("scanner-backlog-lease-loss", async || {
            let (_temp_dirs, writer, other) = test_three_pool_stores_with_isolated_node_contexts(None).await;
            let object = "buckets/.scanner-pause-backlog.json";
            let body = b"native source before lease loss".to_vec();
            let original = writer
                .save_scanner_pause_backlog_replica(2, 1, body.clone(), Default::default())
                .await
                .expect("seed the exact native replica set");
            let (lossy, refresh_calls) = store_with_capacity_lease_loss(&other).await;
            let barrier = PutObjectCommitBarrier::install(RUSTFS_META_BUCKET, object, PutObjectCommitPause::BeforeQuotaRename);
            let put = tokio::spawn(async move {
                lossy
                    .save_scanner_pause_backlog_replica(
                        2,
                        1,
                        b"must not commit after lease loss".to_vec(),
                        crate::storage_api_contracts::object::HTTPPreconditions {
                            if_match: original.etag,
                            ..Default::default()
                        },
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                .await
                .expect("native CAS must reach its commit barrier");
            tokio::time::pause();
            tokio::task::yield_now().await;
            refresh_calls.arm();
            tokio::time::advance(Duration::from_secs(11)).await;
            tokio::task::yield_now().await;
            assert!(
                refresh_calls.load(Ordering::Acquire) > 0,
                "the durable metadata lease must lose refresh quorum"
            );
            barrier.release();
            tokio::time::resume();
            let err = tokio::time::timeout(Duration::from_secs(30), put)
                .await
                .expect("native CAS must finish after the barrier release")
                .expect("native CAS task must not panic")
                .expect_err("a lost outer capacity lease must reject native publication");
            assert!(
                matches!(err, crate::error::Error::NamespaceLockQuorumUnavailable { .. }),
                "unexpected lease error: {err}"
            );
            drop(barrier);
            let mut reader = writer.pools[2].disk_set[1]
                .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the preexisting replica must survive lease loss");
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("read the full retained replica");
            assert_eq!(actual, body);
        });
    }

    #[test]
    #[serial_test::serial]
    fn scanner_backlog_cas_rejects_a_retiring_source_on_a_stale_node() {
        run_large_stack_current_thread_async_test("scanner-backlog-source-fence", async || {
            let (_temp_dirs, store, writer) = test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let object = "buckets/.scanner-pause-backlog.json";
            let body = b"frozen native scanner replica".to_vec();
            let source_set_index = (writer.pools[0].get_disks_by_key(object).set_index + 1) % writer.pools[0].disk_set.len();
            assert_ne!(
                source_set_index,
                writer.pools[0].get_disks_by_key(object).set_index,
                "exercise a non-routed native set"
            );
            let original = writer
                .save_scanner_pause_backlog_replica(
                    0,
                    source_set_index,
                    body.clone(),
                    crate::storage_api_contracts::object::HTTPPreconditions {
                        if_none_match: Some("*".to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed a native scanner replica before retirement");
            assert!(
                writer
                    .scanner_pause_backlog_writable_set_disks()
                    .await
                    .iter()
                    .any(|set| set.pool_index == 0)
            );

            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            let target_total = body.len() * 8;
            set_decommission_capacity_info_overrides_for_test(
                store.id,
                vec![vec![
                    DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len() * 2, body.len() * 2),
                    DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                    DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
                ]],
            );
            store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
                .expect("another node durably retires the selected source");
            assert!(
                writer.pool_meta.read().await.pools[0].decommission.is_none(),
                "the writer must retain a stale snapshot"
            );
            assert!(
                writer
                    .scanner_pause_backlog_writable_set_disks()
                    .await
                    .iter()
                    .any(|set| set.pool_index == 0)
            );

            let result = writer
                .save_scanner_pause_backlog_replica(
                    0,
                    source_set_index,
                    b"late native scanner update".to_vec(),
                    crate::storage_api_contracts::object::HTTPPreconditions {
                        if_match: original.etag.clone(),
                        ..Default::default()
                    },
                )
                .await;
            assert!(
                matches!(result, Err(crate::error::Error::SlowDown)),
                "late native source publication must fail: {result:?}"
            );
            let mut source = writer.pools[0].disk_set[source_set_index]
                .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the original source must remain readable");
            assert_eq!(source.object_info.etag, original.etag);
            let mut actual = Vec::new();
            source
                .read_to_end(&mut actual)
                .await
                .expect("read the entire retained source");
            assert_eq!(actual, body);

            // A new publication captures the surviving membership even while
            // the storage handle retains its old node-local metadata.
            let surviving_pools: Arc<[usize]> = Arc::from([1, 2]);
            for set in &writer.pools[2].disk_set {
                let stale = writer
                    .save_scanner_pause_backlog_replica(
                        2,
                        set.set_index,
                        b"late stale-cohort target update".to_vec(),
                        crate::storage_api_contracts::object::HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect_err("the stale cohort must also fence publication on a healthy target");
                assert!(matches!(stale, crate::error::Error::SlowDown));

                let target_body = format!("surviving native scanner set {}", set.set_index).into_bytes();
                let committed = writer
                    .save_scanner_pause_backlog_replica_for_membership(
                        2,
                        set.set_index,
                        target_body.clone(),
                        crate::storage_api_contracts::object::HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        },
                        Arc::clone(&surviving_pools),
                    )
                    .await
                    .expect("the current surviving cohort must admit every reserved healthy target set");
                let conflict = writer
                    .save_scanner_pause_backlog_replica_for_membership(
                        2,
                        set.set_index,
                        b"must not bypass CAS".to_vec(),
                        crate::storage_api_contracts::object::HTTPPreconditions {
                            if_match: Some("stale-native-revision".to_string()),
                            ..Default::default()
                        },
                        Arc::clone(&surviving_pools),
                    )
                    .await
                    .expect_err("capacity admission must retain the native writer's CAS");
                assert!(matches!(conflict, crate::error::Error::PreconditionFailed));
                let mut target = set
                    .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("read the actual replica set, not the hash-routed set");
                assert_eq!(target.object_info.etag, committed.etag);
                let mut actual = Vec::new();
                target
                    .read_to_end(&mut actual)
                    .await
                    .expect("read the complete native target");
                assert_eq!(actual, target_body);
            }
            assert!(
                writer.pool_meta.read().await.pools[0].decommission.is_none(),
                "valid captured membership must work without refreshing the writer's stale snapshot"
            );
            for (pool_index, set_index) in [(writer.pools.len(), 0), (0, writer.pools[0].disk_set.len())] {
                assert!(matches!(
                    writer
                        .save_scanner_pause_backlog_replica(pool_index, set_index, Vec::new(), Default::default())
                        .await,
                    Err(crate::error::Error::InvalidArgument(_, _, _))
                ));
            }
            store
                .decommission_cancel(0)
                .await
                .expect("cancel retirement before restoring native membership");
            writer
                .save_scanner_pause_backlog_replica(
                    0,
                    source_set_index,
                    b"canceled source membership repair".to_vec(),
                    crate::storage_api_contracts::object::HTTPPreconditions {
                        if_match: original.etag,
                        ..Default::default()
                    },
                )
                .await
                .expect("cancel must retain scanner's existing native membership repair contract");
        });
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_existing_replica_reconciles_capacity_and_cleans_source() {
        data_movement_existing_replica_reconciles_capacity_case(false);
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_existing_replica_outside_reservation_uses_reserved_target() {
        data_movement_existing_replica_reconciles_capacity_case(true);
    }

    fn data_movement_existing_replica_reconciles_capacity_case(existing_outside_reservation: bool) {
        run_large_stack_current_thread_async_test("reserved-replica-reconcile", async move || {
            let (_temp_dirs, store, other_store) =
                test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
            let object = "buckets/reserved-replica-routing.json";
            let body = br#"{"schemaVersion":1,"generation":2}"#.to_vec();
            let old_body = br#"{"schemaVersion":1,"generation":1}"#.to_vec();
            let source_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);
            let target_time = source_time;
            let target_pool_index = if existing_outside_reservation { 1 } else { 2 };
            let mut replicas = vec![(0, body.clone(), source_time), (target_pool_index, old_body, target_time)];
            if existing_outside_reservation {
                replicas.push((
                    2,
                    br#"{"schemaVersion":1,"generation":3}"#.to_vec(),
                    source_time + time::Duration::seconds(10),
                ));
            }
            for (pool_index, payload, mod_time) in replicas.iter().cloned() {
                store.pools[pool_index]
                    .put_object(
                        RUSTFS_META_BUCKET,
                        object,
                        &mut PutObjReader::from_vec(payload),
                        &ObjectOptions {
                            max_parity: true,
                            mod_time: Some(mod_time),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed existing replicas with independent write times");
            }
            let layout = DecommissionErasureLayout { data: 1, parity: 0 };
            let target_total = body.len() * 8;
            let capacities = vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len() * 2, body.len() * 2),
                DecommissionPoolCapacityInfo::for_test(
                    1,
                    layout,
                    if existing_outside_reservation { target_total } else { 0 },
                    target_total,
                    if existing_outside_reservation { 0 } else { target_total },
                ),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ];
            set_decommission_capacity_info_overrides_for_test(store.id, vec![capacities.clone()]);
            store
                .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
                .await
                .expect("activate the source reservation");
            let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
            let reserved_snapshot = store.pool_meta.read().await.clone();
            let reservation = reserved_snapshot.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("active source reservation");
            assert_eq!(
                reservation.targets.iter().map(|target| target.pool_index).collect::<Vec<_>>(),
                vec![target_pool_index],
                "the fixture must reserve exactly one target"
            );
            let source_reader = store.pools[0]
                .get_object_reader(
                    RUSTFS_META_BUCKET,
                    object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        no_lock: true,
                        data_movement: true,
                        raw_data_movement_read: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read the frozen source replica");
            let conflict = data_movement::migrate_decommission_object(
                Arc::clone(&store),
                0,
                RUSTFS_META_BUCKET.to_string(),
                source_reader,
                None,
                "reserved_replica_conflict",
                Some(owner),
            )
            .await
            .expect_err("a different older existing record must retain its source and capacity intent");
            assert!(conflict.to_string().contains("Precondition failed"), "unexpected conflict: {conflict}");
            let reserved_snapshot = store.pool_meta.read().await.clone();
            let mut selection_opts = ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                ..Default::default()
            };
            assert_eq!(
                store
                    .select_data_movement_pool_idx(RUSTFS_META_BUCKET, object, body.len() as i64, &selection_opts, true)
                    .await
                    .expect("selection without a capacity owner retains existing-replica routing"),
                2
            );
            owner.apply_to(&mut selection_opts);
            for stale_owner in [
                DecommissionCapacityOwner {
                    owner_nonce: uuid::Uuid::new_v4(),
                    ..owner
                },
                DecommissionCapacityOwner {
                    generation: owner.generation + 1,
                    ..owner
                },
            ] {
                let mut stale_opts = selection_opts.clone();
                stale_owner.apply_to(&mut stale_opts);
                assert!(
                    matches!(
                        store
                            .select_data_movement_pool_idx(RUSTFS_META_BUCKET, object, body.len() as i64, &stale_opts, true)
                            .await,
                        Err(crate::error::Error::DecommissionCapacityBlocked { .. })
                    ),
                    "a stale owner must not fall back to another target"
                );
            }
            {
                let mut meta = store.pool_meta.write().await;
                meta.pools[0]
                    .decommission
                    .as_mut()
                    .unwrap()
                    .capacity_reservation
                    .as_mut()
                    .unwrap()
                    .expires_at = time::OffsetDateTime::now_utc() - time::Duration::seconds(1);
            }
            assert!(
                matches!(
                    store
                        .select_data_movement_pool_idx(RUSTFS_META_BUCKET, object, body.len() as i64, &selection_opts, true)
                        .await,
                    Err(crate::error::Error::DecommissionCapacityBlocked { .. })
                ),
                "an expired owner must not fall back to another target"
            );
            *store.pool_meta.write().await = reserved_snapshot.clone();
            if !existing_outside_reservation {
                {
                    let mut meta = store.pool_meta.write().await;
                    let target = &mut meta.pools[0]
                        .decommission
                        .as_mut()
                        .unwrap()
                        .capacity_reservation
                        .as_mut()
                        .unwrap()
                        .targets[0];
                    target.consumed_physical_bytes = target.reserved_physical_bytes;
                }
                assert_eq!(
                    store
                        .select_data_movement_pool_idx(RUSTFS_META_BUCKET, object, body.len() as i64, &selection_opts, true)
                        .await
                        .expect("an existing reserved replica can still be selected after capacity was consumed"),
                    target_pool_index
                );
                *store.pool_meta.write().await = reserved_snapshot;
            }
            let mut persisted = crate::core::pools::PoolMeta::default();
            persisted
                .load_no_lock_from_replicas(store.pools.clone())
                .await
                .expect("reload the unresolved intent");
            assert_eq!(
                persisted.pools[0]
                    .decommission
                    .as_ref()
                    .expect("source state")
                    .capacity_reservation
                    .as_ref()
                    .expect("durable capacity")
                    .pending_target_physical_bytes,
                body.len()
            );
            for (pool_index, payload, mod_time) in &replicas {
                let mut reader = store.pools[*pool_index]
                    .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("a refused existing record replacement must preserve every replica");
                assert_eq!(reader.object_info.mod_time, Some(*mod_time));
                let mut actual = Vec::new();
                reader
                    .read_to_end(&mut actual)
                    .await
                    .expect("read the unchanged existing record");
                assert_eq!(&actual, payload);
            }
            let previous = store.pools[target_pool_index]
                .get_object_info(RUSTFS_META_BUCKET, object, &ObjectOptions::default())
                .await
                .expect("read the existing writer's CAS revision");
            let replacement = store.pools[target_pool_index]
                .put_object(
                    RUSTFS_META_BUCKET,
                    object,
                    &mut PutObjReader::from_vec(body.clone()),
                    &ObjectOptions {
                        max_parity: true,
                        mod_time: Some(target_time),
                        http_preconditions: Some(crate::storage_api_contracts::object::HTTPPreconditions {
                            if_match: previous.etag,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
                .expect("existing CAS converges the payload without a migration marker");
            assert!(!data_movement::is_owned_data_movement_target(&replacement));
            *other_store.pool_meta.write().await = persisted;
            set_decommission_capacity_info_overrides_for_test(other_store.id, vec![capacities]);
            tokio::time::timeout(
                Duration::from_secs(30),
                other_store.decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    RUSTFS_META_BUCKET.to_string(),
                    other_store.pools[0].get_disks_by_key(object),
                ),
            )
            .await
            .expect("replica conflict recovery must be bounded")
            .expect("identical existing replica should finish migration on the reloaded node");
            let mut reconciled = crate::core::pools::PoolMeta::default();
            reconciled
                .load_no_lock_from_replicas(other_store.pools.clone())
                .await
                .expect("reload reconciled capacity");
            let reservation = reconciled.pools[0]
                .decommission
                .as_ref()
                .expect("source state")
                .capacity_reservation
                .as_ref()
                .expect("reconciled capacity");
            assert_eq!(reservation.pending_target_physical_bytes, 0);
            assert_eq!(reservation.committed_data_bytes, body.len());
            assert_eq!(reservation.consumed_target_physical_bytes, body.len());
            assert!(reservation.targets.iter().all(|target| target.pending_mutation_id.is_none()));
            assert_eq!(
                other_store.pool_meta.read().await.pools[0]
                    .decommission
                    .as_ref()
                    .expect("worker progress")
                    .items_decommission_failed,
                0
            );
            let missing = other_store.pools[0]
                .get_object_info(RUSTFS_META_BUCKET, object, &ObjectOptions::default())
                .await
                .expect_err("the source should be cleaned only after equivalent-target capacity reconciliation");
            assert!(crate::error::is_err_object_not_found(&missing));
            let mut target_reader = other_store.pools[target_pool_index]
                .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("the surviving replica should remain readable");
            assert_eq!(
                target_reader.object_info.mod_time,
                Some(target_time),
                "recovery must not overwrite the existing target"
            );
            let mut actual = Vec::new();
            target_reader
                .read_to_end(&mut actual)
                .await
                .expect("read surviving ledger bytes");
            assert_eq!(actual, body);
            if existing_outside_reservation {
                let (_, outside_body, outside_time) = replicas.last().expect("unreserved existing replica");
                let mut outside = other_store.pools[2]
                    .get_object_reader(RUSTFS_META_BUCKET, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("migration must leave the unreserved existing replica intact");
                assert_eq!(outside.object_info.mod_time, Some(*outside_time));
                let mut actual = Vec::new();
                outside
                    .read_to_end(&mut actual)
                    .await
                    .expect("read the untouched unreserved replica");
                assert_eq!(&actual, outside_body);
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn data_movement_equivalent_target_reconciles_published_capacity_after_restart() {
        run_large_stack_current_thread_async_test(
            "equivalent-target-capacity-restart",
            data_movement_equivalent_target_reconciles_published_capacity_after_restart_case,
        );
    }

    async fn data_movement_equivalent_target_reconciles_published_capacity_after_restart_case() {
        let (_temp_dirs, store, other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("equivalent-target");
        let object = "published-before-capacity-save.bin";
        let interleaved_object = "interleaved-before-capacity-reconcile.bin";
        let next_object = "next-after-capacity-reconcile.bin";
        let body = vec![0x54; 256 * 1024];
        let source_version = uuid::Uuid::new_v4().to_string();
        let interleaved_source_version = uuid::Uuid::new_v4().to_string();
        let next_source_version = uuid::Uuid::new_v4().to_string();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the equivalent-target reconciliation bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the equivalent-target reconciliation bucket incarnation");

        let mut source_data = PutObjReader::from_vec(body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_data,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the source object whose target will publish before the save");
        let mut interleaved_source_data = PutObjReader::from_vec(body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                interleaved_object,
                &mut interleaved_source_data,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(interleaved_source_version.clone()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the interleaved source object");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the source capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the source object for the first migration attempt");
        let barrier = PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::AfterRenameHandoff);
        let migration = tokio::spawn({
            let migration_store = Arc::clone(&lossy_store);
            let migration_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "equivalent_target_reconcile_initial",
                    Some(owner),
                )
                .await
            }
        });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        let published = lossy_store.pools[2]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the target must be published before capacity progress save");
        assert_eq!(published.version_id.map(|version| version.to_string()), Some(source_version.clone()));
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the initial capacity progress save must observe its lost lease"
        );
        barrier.release();
        tokio::time::resume();
        let initial_err = tokio::time::timeout(Duration::from_secs(30), migration)
            .await
            .expect("the initial migration should finish after the commit barrier release")
            .expect("the initial migration task should not panic")
            .expect_err("the published target must report its failed capacity progress save");
        assert!(!initial_err.to_string().is_empty(), "the failed capacity save must remain observable");
        drop(barrier);

        let mut failed_persisted = crate::core::pools::PoolMeta::default();
        failed_persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the published target failure must leave readable durable metadata");
        let failed_reservation = failed_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the failed target capacity intent must remain durable");
        assert!(failed_reservation.pending_target_physical_bytes > 0);
        assert!(
            failed_reservation
                .targets
                .iter()
                .any(|target| target.pending_mutation_id.is_some())
        );
        assert_eq!(failed_reservation.consumed_target_physical_bytes, 0);

        *other_store.pool_meta.write().await = failed_persisted.clone();
        set_decommission_capacity_info_overrides_for_test(
            other_store.id,
            vec![
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
            ],
        );
        let retry_owner = decommission_capacity_owner(&failed_persisted);
        let interleaved_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                interleaved_object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(interleaved_source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the interleaved source object while the first intent is pending");
        let interleaved_result = tokio::time::timeout(
            Duration::from_secs(30),
            data_movement::migrate_decommission_object(
                Arc::clone(&other_store),
                0,
                bucket.clone(),
                interleaved_reader,
                Some(incarnation),
                "equivalent_target_reconcile_interleaved",
                Some(retry_owner),
            ),
        )
        .await
        .expect("a different mutation must be rejected without waiting on the pending intent");
        assert!(interleaved_result.is_err(), "B must not reuse A's pending capacity intent");
        assert!(
            interleaved_result
                .as_ref()
                .expect_err("B must not succeed while A's pending intent is unresolved")
                .to_string()
                .contains("unresolved target capacity intent")
        );
        let interleaved_target_err = other_store.pools[2]
            .get_object_info(
                &bucket,
                interleaved_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(interleaved_source_version),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("B must not publish while A's pending intent is unresolved");
        assert!(
            crate::error::is_err_object_not_found(&interleaved_target_err)
                || crate::error::is_err_version_not_found(&interleaved_target_err)
        );
        let target_lock = other_store
            .new_ns_lock(RUSTFS_META_BUCKET, &format!("{DECOMMISSION_CAPACITY_TARGET_LOCK_PREFIX}/2"))
            .await
            .expect("create the equivalent-target retry gate");
        let target_guard = target_lock
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("hold the equivalent-target gate before restart retry");
        let retry_observer = DecommissionCapacityLockOrderBarrier::install(other_store.id, other_store.id);
        retry_observer.disable_owner_pause();
        let retry_set = other_store.pools[0].get_disks_by_key(object);
        let mut retry = tokio::spawn({
            let retry_store = Arc::clone(&other_store);
            let retry_bucket = bucket.clone();
            async move {
                retry_store
                    .decommission_entry_for_test(
                        0,
                        MetaCacheEntry {
                            name: object.to_string(),
                            ..Default::default()
                        },
                        retry_bucket,
                        retry_set,
                    )
                    .await
            }
        });
        tokio::select! {
            _ = retry_observer.wait_until_target_gate_retry() => {}
            result = &mut retry => panic!("equivalent-target retry finished before target contention: {result:?}"),
        }
        drop(target_guard);
        tokio::time::timeout(Duration::from_secs(30), retry)
            .await
            .expect("equivalent-target retry should not deadlock after permit transfer")
            .expect("equivalent-target retry task should not panic")
            .expect("equivalent target retry should reconcile the pending capacity intent");
        assert_eq!(
            retry_observer.target_gate_exact_reloads(),
            1,
            "equivalent reconciliation must consume its exact target permit without re-entering busy recovery"
        );
        drop(retry_observer);

        let mut reconciled = crate::core::pools::PoolMeta::default();
        reconciled
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reconciled capacity metadata should remain durable");
        let reconciled_reservation = reconciled.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("reconciled capacity reservation should remain present");
        assert_eq!(reconciled_reservation.pending_target_physical_bytes, 0);
        assert_eq!(reconciled_reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reconciled_reservation.consumed_target_physical_bytes, body.len());
        assert_eq!(reconciled_reservation.committed_data_bytes, body.len());

        let mut next_source_data = PutObjReader::from_vec(body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                next_object,
                &mut next_source_data,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_source_version.clone()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the next source object for the capacity oracle");
        let next_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                next_object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the next source object for the capacity oracle");
        let next_result = tokio::time::timeout(
            Duration::from_secs(30),
            data_movement::migrate_decommission_object(
                Arc::clone(&other_store),
                0,
                bucket.clone(),
                next_reader,
                Some(incarnation),
                "equivalent_target_reconcile_next_object",
                Some(retry_owner),
            ),
        )
        .await
        .expect("the next object capacity oracle should finish promptly");
        assert!(next_result.is_err(), "consumed capacity must reject the next exact-fit object");
        let next_target_err = other_store.pools[2]
            .get_object_info(
                &bucket,
                next_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_source_version),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("the next object must not publish after the reservation is consumed");
        assert!(
            crate::error::is_err_object_not_found(&next_target_err) || crate::error::is_err_version_not_found(&next_target_err),
            "capacity rejection must leave the next target absent: {next_target_err}"
        );
        let mut final_persisted = crate::core::pools::PoolMeta::default();
        final_persisted
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("the next-object capacity rejection must preserve durable reconciliation");
        let final_reservation = final_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the final capacity reservation should remain present");
        assert_eq!(final_reservation.pending_target_physical_bytes, 0);
        assert_eq!(final_reservation.consumed_target_physical_bytes, body.len());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_equivalent_delete_marker_reconciles_pending_capacity_after_restart() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("equivalent-delete-marker");
        let object = "published-delete-marker.bin";
        let next_object = "next-delete-marker.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the equivalent delete-marker bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the equivalent delete-marker bucket incarnation");

        let mut source_body = PutObjReader::from_vec(b"delete marker source".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_body,
                &ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the source object");
        let source_marker = store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("create the source delete marker");
        let source_version_id = source_marker.version_id.expect("source marker must have a version id");
        let source_mod_time = source_marker.mod_time.expect("source marker must have a modification time");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1, 1),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 1, 1),
                DecommissionPoolCapacityInfo::for_test(2, layout, 2, 2, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the delete-marker capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let mut move_opts = ObjectOptions {
            data_movement: true,
            delete_marker: true,
            versioned: true,
            version_id: Some(source_version_id.to_string()),
            mod_time: Some(source_mod_time),
            src_pool_idx: 0,
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut move_opts);
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
                capacity_snapshot(),
            ],
        );
        let barrier = DeleteObjectCommitBarrier::install_after_publish(&bucket, object);
        let publish_store = Arc::clone(&lossy_store);
        let publish_bucket = bucket.clone();
        let publish_opts = move_opts.clone();
        let publish = tokio::spawn(async move { publish_store.delete_object(&publish_bucket, object, publish_opts).await });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        barrier.release();
        let publish_err = tokio::time::timeout(Duration::from_secs(30), publish)
            .await
            .expect("the published marker should finish after the deterministic barrier release")
            .expect("the published marker task should not panic")
            .expect_err("the final capacity progress save must observe the lost lease");
        assert!(!publish_err.to_string().is_empty());
        tokio::time::resume();
        drop(barrier);
        let published = lossy_store.pools[2]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version_id.to_string()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("the target marker must be visible before the simulated restart");
        assert!(published.delete_marker);

        let mut failed_persisted = crate::core::pools::PoolMeta::default();
        failed_persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("published marker metadata should remain readable");
        let failed_reservation = failed_persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the source reservation should remain durable");
        assert_eq!(failed_reservation.pending_target_physical_bytes, 1);
        assert_eq!(failed_reservation.consumed_target_physical_bytes, 0);
        assert!(
            failed_reservation
                .targets
                .iter()
                .any(|target| target.pending_mutation_id.is_some())
        );
        *other_store.pool_meta.write().await = failed_persisted.clone();
        let mut restart_persisted = crate::core::pools::PoolMeta::default();
        restart_persisted
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("the pending marker intent should survive restart");
        assert_eq!(
            restart_persisted.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("the restarted reservation should exist")
                .pending_target_physical_bytes,
            1
        );
        *other_store.pool_meta.write().await = restart_persisted.clone();

        let mut next_source_body = PutObjReader::from_vec(b"next delete marker".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                next_object,
                &mut next_source_body,
                &ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the next source object");
        let next_source_marker = store.pools[0]
            .delete_object(
                &bucket,
                next_object,
                ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("create the next source delete marker");
        let next_source_version_id = next_source_marker.version_id.expect("next marker version id");
        let next_owner = decommission_capacity_owner(&restart_persisted);
        let mut next_opts = ObjectOptions {
            data_movement: true,
            delete_marker: true,
            versioned: true,
            version_id: Some(next_source_version_id.to_string()),
            mod_time: next_source_marker.mod_time,
            src_pool_idx: 0,
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        next_owner.apply_to(&mut next_opts);
        let next_before_reconcile = other_store
            .delete_object(&bucket, next_object, next_opts.clone())
            .await
            .expect_err("a next mutation must not reuse the pending marker intent");
        assert!(next_before_reconcile.to_string().contains("decommission_capacity_blocked"));

        other_store
            .delete_object(&bucket, object, move_opts)
            .await
            .expect("the equivalent target retry must reconcile the pending marker intent");
        let mut reconciled = crate::core::pools::PoolMeta::default();
        reconciled
            .load_no_lock_from_replicas(other_store.pools.clone())
            .await
            .expect("reconciled marker metadata should remain durable");
        let reconciled_reservation = reconciled.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the reconciled reservation should exist");
        assert_eq!(reconciled_reservation.pending_target_physical_bytes, 0);
        assert_eq!(reconciled_reservation.consumed_target_physical_bytes, 1);

        let next_after_reconcile = other_store
            .delete_object(&bucket, next_object, next_opts)
            .await
            .expect_err("the consumed exact-fit capacity must reject the next marker");
        assert!(next_after_reconcile.to_string().contains("decommission_capacity_blocked"));
        let next_target_err = other_store.pools[2]
            .get_object_info(
                &bucket,
                next_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(next_source_version_id.to_string()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the rejected next marker must remain unpublished");
        assert!(
            crate::error::is_err_object_not_found(&next_target_err) || crate::error::is_err_version_not_found(&next_target_err)
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_delete_marker_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("delete-marker-loss");
        let object = "delete-marker-lease-loss.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create delete-marker lease-loss bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load delete-marker lease-loss bucket incarnation");
        let mut body = PutObjReader::from_vec(b"delete-marker source".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut body,
                &ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the source object for delete-marker movement");
        let source_marker = store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("create the source delete marker");
        assert!(source_marker.delete_marker, "the source version must be a delete marker");
        let source_version_id = source_marker
            .version_id
            .expect("the source delete marker must have a version id");
        let source_mod_time = source_marker
            .mod_time
            .expect("the source delete marker must have a modification time");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 1024, 1024),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, 4096, 4096),
                DecommissionPoolCapacityInfo::for_test(2, layout, 4096, 4096, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the delete-marker capacity reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        assert_eq!(
            store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("delete-marker reservation should exist")
                .targets[0]
                .pool_index,
            2
        );

        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let mut move_opts = ObjectOptions {
            data_movement: true,
            delete_marker: true,
            versioned: true,
            version_id: Some(source_version_id.to_string()),
            mod_time: Some(source_mod_time),
            src_pool_idx: 0,
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut move_opts);
        let barrier = DeleteObjectCommitBarrier::install(&bucket, object);
        let delete_store = Arc::clone(&lossy_store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move { delete_store.delete_object(&delete_bucket, object, move_opts).await });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the delete-marker capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), delete)
            .await
            .expect("delete-marker movement should finish after the commit barrier release")
            .expect("delete-marker movement task should not panic")
            .expect_err("lost delete-marker capacity lease must fence before publication");
        assert!(!err.to_string().is_empty(), "delete-marker lease loss should remain observable");
        drop(barrier);

        let target_err = lossy_store.pools[2]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version_id.to_string()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect_err("lost delete-marker capacity lease must not publish the target marker");
        assert!(
            crate::error::is_err_object_not_found(&target_err) || crate::error::is_err_version_not_found(&target_err),
            "target delete marker should remain absent after the fenced failure: {target_err}"
        );
        let source = lossy_store.pools[0]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version_id.to_string()),
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("the source delete marker must remain intact");
        assert!(source.delete_marker);
        assert_eq!(source.mod_time, Some(source_mod_time));

        let meta = lossy_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("delete-marker reservation should remain after the fenced failure");
        assert!(reservation.pending_target_physical_bytes > 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reservation.consumed_target_physical_bytes, 0);
        let live_progress = (
            reservation.pending_target_physical_bytes,
            reservation.inflight_target_physical_bytes,
            reservation.consumed_target_physical_bytes,
            reservation.observed_target_physical_bytes,
            reservation.committed_data_bytes,
        );
        drop(meta);

        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the fenced delete-marker failure should leave readable pool metadata");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the fenced delete-marker reservation should remain persisted");
        assert_eq!(
            (
                persisted_reservation.pending_target_physical_bytes,
                persisted_reservation.inflight_target_physical_bytes,
                persisted_reservation.consumed_target_physical_bytes,
                persisted_reservation.observed_target_physical_bytes,
                persisted_reservation.committed_data_bytes,
            ),
            live_progress,
            "fenced delete-marker progress must match its durable replica"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_upload_part_fences_capacity_lease_loss_before_publication() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let object = "data-movement-part-lease-loss.bin";
        let body = vec![0x63; 256 * 1024];
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body.len().saturating_mul(4);
        let capacity_snapshot = || {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body.len(), body.len()),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot()]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the data-movement UploadPart reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let reservation_target = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement UploadPart reservation should exist")
            .targets[0]
            .pool_index;
        assert_eq!(reservation_target, 2, "data-movement UploadPart should use the reserved target");

        let upload = new_multipart_upload(
            &store,
            2,
            RUSTFS_META_BUCKET,
            object,
            ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                versioned: true,
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("create the data-movement UploadPart target upload");
        let (lossy_store, refresh_calls) = store_with_capacity_lease_loss(&other_store).await;
        set_decommission_capacity_info_overrides_for_test(
            lossy_store.id,
            vec![capacity_snapshot(), capacity_snapshot(), capacity_snapshot()],
        );
        let mut part_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            part_number: Some(1),
            ..ObjectOptions::with_capacity_expected_data_bytes(Some(body.len()))
        };
        owner.apply_to(&mut part_opts);
        let barrier = MultipartCommitBarrier::install(RUSTFS_META_BUCKET, object, MultipartCommitPause::PutPartBeforeLockLost);
        let part_store = Arc::clone(&lossy_store);
        let part_upload_id = upload.upload_id.clone();
        let part_opts = part_opts.clone();
        let part_body = body.clone();
        let part = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(part_body);
            part_store
                .put_object_part_for_data_movement(2, RUSTFS_META_BUCKET, object, &part_upload_id, &mut data, &part_opts)
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::pause();
        tokio::task::yield_now().await;
        refresh_calls.arm();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "the data-movement UploadPart capacity lease must lose refresh quorum"
        );
        barrier.release();
        let err = tokio::time::timeout(Duration::from_secs(30), part)
            .await
            .expect("data-movement UploadPart should finish after the commit barrier release")
            .expect("data-movement UploadPart task should not panic")
            .expect_err("a lost data-movement UploadPart capacity lease must fence before publication");
        assert!(
            !err.to_string().is_empty(),
            "data-movement UploadPart lease loss should remain observable"
        );
        drop(barrier);

        let listed = lossy_store.pools[2]
            .list_object_parts(
                RUSTFS_META_BUCKET,
                object,
                &upload.upload_id,
                None,
                100,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the fenced data-movement UploadPart should leave the MPU readable");
        assert!(
            listed.parts.is_empty(),
            "lost data-movement UploadPart capacity lease must not publish a part"
        );

        let meta = lossy_store.pool_meta.read().await;
        let reservation = meta.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("data-movement UploadPart reservation should remain after the fenced failure");
        assert!(reservation.pending_target_physical_bytes > 0);
        assert_eq!(reservation.inflight_target_physical_bytes, 0);
        assert_eq!(reservation.consumed_target_physical_bytes, 0);
        let live_progress = (
            reservation.pending_target_physical_bytes,
            reservation.inflight_target_physical_bytes,
            reservation.consumed_target_physical_bytes,
            reservation.observed_target_physical_bytes,
            reservation.committed_data_bytes,
        );
        drop(meta);

        let mut persisted = crate::core::pools::PoolMeta::default();
        persisted
            .load_no_lock_from_replicas(lossy_store.pools.clone())
            .await
            .expect("the fenced data-movement UploadPart failure should leave readable pool metadata");
        let persisted_reservation = persisted.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("the fenced data-movement UploadPart reservation should remain persisted");
        assert_eq!(
            (
                persisted_reservation.pending_target_physical_bytes,
                persisted_reservation.inflight_target_physical_bytes,
                persisted_reservation.consumed_target_physical_bytes,
                persisted_reservation.observed_target_physical_bytes,
                persisted_reservation.committed_data_bytes,
            ),
            live_progress,
            "fenced data-movement UploadPart progress must match its durable replica"
        );
    }

    async fn assert_lock_order_with_tail(mutation: ExternalObjectMutation, pause_restore_tail: bool) {
        let (_temp_dirs, store, other_store) = if pause_restore_tail {
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await
        } else {
            test_three_pool_stores_with_isolated_node_contexts(None).await
        };
        let bucket = test_bucket(&format!("{}-order", mutation.label()));
        let object = "shared-object.bin";
        let copy_source_object = "copy-source.bin";
        let source_version = uuid::Uuid::new_v4().to_string();
        let source_body = b"decommission source body".to_vec();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create lock-order bucket");
        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("load bucket incarnation");

        let mut existing = PutObjReader::from_vec(b"existing unreserved target".to_vec());
        let existing_info = store.pools[2]
            .put_object(&bucket, object, &mut existing, &ObjectOptions::default())
            .await
            .expect("seed the ordinary PUT target");
        let mut source = PutObjReader::from_vec(source_body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the decommission source version");

        let restore_get_barrier = if matches!(mutation, ExternalObjectMutation::Restore) {
            let tier_name = format!("ORDERRESTORE{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
            let source_tiers = store.tier_config_mgr();
            let restore_tiers = other_store.tier_config_mgr();
            let backend = register_mock_tier(&source_tiers, &tier_name).await;
            // Both peers must resolve the same persisted backend identity, including its prefix.
            let tier_config = source_tiers.read().await.tiers[&tier_name].clone();
            restore_tiers.write().await.tiers.insert(tier_name.clone(), tier_config);
            crate::services::tier::tier::TierConfigMgr::install_test_driver_in(
                &restore_tiers,
                &tier_name,
                Box::new(backend.clone()),
            )
            .await
            .expect("install the same mock tier on the restoring peer");
            store.pools[2]
                .transition_object(
                    &bucket,
                    object,
                    &ObjectOptions {
                        no_lock: true,
                        transition: crate::bucket::lifecycle::lifecycle::TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name,
                            etag: existing_info.etag.clone().expect("the restore target should have an ETag"),
                            ..Default::default()
                        },
                        mod_time: existing_info.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed the transitioned restore target");
            Some(backend.arm_get_barrier().await)
        } else {
            None
        };

        let delete_marker_version = if matches!(mutation, ExternalObjectMutation::Delete) {
            let marker = store.pools[2]
                .delete_object(
                    &bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed the ordinary target delete marker");
            Some(
                marker
                    .version_id
                    .expect("the versioned target delete should return its marker version")
                    .to_string(),
            )
        } else {
            None
        };

        let prepared_multipart = if matches!(mutation, ExternalObjectMutation::CompleteMultipart) {
            let upload = new_multipart_upload(
                &store,
                2,
                &bucket,
                object,
                ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("create the ordinary multipart upload");
            let multipart_opts = multipart_options(
                &store,
                &bucket,
                ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await;
            let mut data = PutObjReader::from_vec(b"ordinary multipart replacement".to_vec());
            let part = store.pools[2]
                .put_object_part(&bucket, object, &upload.upload_id, 1, &mut data, &multipart_opts)
                .await
                .expect("stage the ordinary multipart part");
            Some((
                upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
            ))
        } else {
            None
        };
        let copy_body_size = if matches!(mutation, ExternalObjectMutation::Copy) {
            if pause_restore_tail {
                256 * 1024
            } else {
                b"ordinary copy replacement".len()
            }
        } else {
            0
        };
        let prepared_copy = if matches!(mutation, ExternalObjectMutation::Copy) {
            let copy_body = vec![0x6a; copy_body_size];
            let mut copy_source = PutObjReader::from_vec(copy_body.clone());
            store.pools[2]
                .put_object(&bucket, copy_source_object, &mut copy_source, &ObjectOptions::default())
                .await
                .expect("seed the ordinary copy source");
            let mut copy_reader = store.pools[2]
                .get_object_reader(
                    &bucket,
                    copy_source_object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read the ordinary copy source");
            let mut copy_info = copy_reader.object_info.clone();
            let mut copy_data = Vec::new();
            copy_reader
                .stream
                .read_to_end(&mut copy_data)
                .await
                .expect("drain the ordinary copy source");
            assert_eq!(copy_data, copy_body);
            copy_info.put_object_reader = Some(PutObjReader::from_vec(copy_data));
            Some(copy_info)
        } else {
            None
        };

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = source_body.len().max(copy_body_size) * 4;
        let capacity_snapshot = |target_free| {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, source_body.len(), source_body.len()),
                DecommissionPoolCapacityInfo::for_test(
                    1,
                    layout,
                    target_free,
                    target_total,
                    target_total.saturating_sub(target_free),
                ),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot(target_total)]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the source reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);

        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![
                capacity_snapshot(target_total),
                capacity_snapshot(target_total),
                capacity_snapshot(target_total - source_body.len()),
            ],
        );
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the exact source version");
        let barrier = DecommissionCapacityLockOrderBarrier::install(store.id, other_store.id);
        let mut migration = tokio::spawn({
            let migration_store = Arc::clone(&store);
            let migration_bucket = bucket.clone();
            async move {
                let result = data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "decommission_object_lock_order",
                    Some(owner),
                )
                .await;
                (result, std::time::Instant::now())
            }
        });
        barrier.wait_until_owner_paused().await;

        let ordinary_launched_at = std::time::Instant::now();
        let mut ordinary_mutation = tokio::spawn({
            let ordinary_store = Arc::clone(&other_store);
            let ordinary_bucket = bucket.clone();
            async move {
                let ordinary_started_at = std::time::Instant::now();
                let result = match mutation {
                    ExternalObjectMutation::Put => {
                        let mut data = PutObjReader::from_vec(b"ordinary replacement".to_vec());
                        ordinary_store
                            .put_object(&ordinary_bucket, object, &mut data, &ObjectOptions::default())
                            .await
                            .map(|_| ())
                    }
                    ExternalObjectMutation::CompleteMultipart => {
                        let (upload_id, completed_parts) =
                            prepared_multipart.expect("multipart mutation should have a staged upload");
                        ordinary_store
                            .complete_multipart_upload(
                                &ordinary_bucket,
                                object,
                                &upload_id,
                                completed_parts,
                                &ObjectOptions::default(),
                            )
                            .await
                            .map(|_| ())
                    }
                    ExternalObjectMutation::Copy => {
                        let mut copy_info = prepared_copy.expect("copy mutation should have a source reader");
                        ordinary_store
                            .copy_object(
                                &ordinary_bucket,
                                copy_source_object,
                                &ordinary_bucket,
                                object,
                                &mut copy_info,
                                &ObjectOptions::default(),
                                &ObjectOptions::default(),
                            )
                            .await
                            .map(|_| ())
                    }
                    ExternalObjectMutation::PutTags => ordinary_store
                        .put_object_tags(&ordinary_bucket, object, "ordinary=tag", &ObjectOptions::default())
                        .await
                        .map(|_| ()),
                    ExternalObjectMutation::DeleteTags => ordinary_store
                        .delete_object_tags(&ordinary_bucket, object, &ObjectOptions::default())
                        .await
                        .map(|_| ()),
                    ExternalObjectMutation::PutMetadata => ordinary_store
                        .put_object_metadata(&ordinary_bucket, object, &ObjectOptions::default())
                        .await
                        .map(|_| ()),
                    ExternalObjectMutation::Delete => {
                        let version_id = delete_marker_version.expect("delete mutation should have a marker version");
                        ordinary_store
                            .delete_object(
                                &ordinary_bucket,
                                object,
                                ObjectOptions {
                                    versioned: true,
                                    version_id: Some(version_id.clone()),
                                    expected_current_version_id: Some(version_id),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map(|_| ())
                    }
                    ExternalObjectMutation::Restore => {
                        let mut opts = ObjectOptions::default();
                        opts.transition.restore_request.days = Some(1);
                        ordinary_store
                            .restore_transitioned_object(&ordinary_bucket, object, &opts)
                            .await
                    }
                };
                (result, ordinary_started_at, std::time::Instant::now())
            }
        });
        if let Some(get_barrier) = restore_get_barrier.as_ref() {
            tokio::select! {
                () = get_barrier.wait_until_paused() => {}
                result = &mut ordinary_mutation => panic!("restore finished before the tier GET barrier: {result:?}"),
            }
            let read_opts = ObjectOptions {
                skip_decommissioned: true,
                ..Default::default()
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                other_store.get_object_info(&bucket, object, &read_opts),
            )
            .await
            .expect("HEAD must not wait for the paused tier restore download")
            .expect("HEAD should remain readable during the paused tier restore download");
            let mut reader = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                other_store.get_object_reader(&bucket, object, None, HeaderMap::new(), &read_opts),
            )
            .await
            .expect("GET must not wait for the paused tier restore download")
            .expect("GET should remain readable during the paused tier restore download");
            let mut body = Vec::new();
            tokio::time::timeout(std::time::Duration::from_secs(1), reader.stream.read_to_end(&mut body))
                .await
                .expect("GET body must not wait for the paused tier restore download")
                .expect("GET body should remain readable during the paused tier restore download");
            assert_eq!(body, b"existing unreserved target");
            get_barrier.release();
        }
        if matches!(
            mutation,
            ExternalObjectMutation::Restore
                | ExternalObjectMutation::Put
                | ExternalObjectMutation::CompleteMultipart
                | ExternalObjectMutation::Copy
        ) {
            if pause_restore_tail {
                barrier.pause_external_object_commit_phase();
            }
            barrier.wait_until_external_object_commit_phase_started().await;
        } else {
            barrier.wait_until_external_capacity_released().await;
        }
        let ordinary_lock_phase_observed_at = std::time::Instant::now();
        assert!(!ordinary_mutation.is_finished());
        let owner_released_at = std::time::Instant::now();
        let (migration_result, migration_finished_at) = if pause_restore_tail {
            barrier.release_owner();
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut migration)
                .await
                .expect("migration must finish before the early-ACK tail is released")
                .expect("migration task should join")
        } else {
            barrier.release_owner();
            tokio::time::timeout(std::time::Duration::from_secs(30), migration)
                .await
                .expect("migration must not deadlock with the ordinary object mutation")
                .expect("migration task should join")
        };
        migration_result.expect("migration should commit to its reserved target");

        let (ordinary_result, ordinary_started_at, ordinary_finished_at) = if pause_restore_tail {
            let handoff_barrier = PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::AfterRenameHandoff);
            let tail_barrier =
                crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
            barrier.release_external_object_commit_phase();
            tokio::time::timeout(std::time::Duration::from_secs(30), handoff_barrier.wait_until_paused())
                .await
                .expect("restore should reach the quorum handoff before the tail probe");
            handoff_barrier.release();
            drop(handoff_barrier);
            tokio::time::timeout(std::time::Duration::from_secs(30), tail_barrier.wait_until_paused())
                .await
                .expect("restore should pause a tail rename after quorum publication");
            let object_lock = other_store
                .new_ns_lock(&bucket, object)
                .await
                .expect("create the shared-domain object guard probe");
            let mut object_probe =
                tokio::spawn(async move { object_lock.get_write_lock(std::time::Duration::from_secs(30)).await });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(50), &mut object_probe)
                    .await
                    .is_err(),
                "early-ACK tail must retain the decommission namespace guard"
            );

            let capacity_lock = other_store
                .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
                .await
                .expect("create the decommission capacity guard probe");
            let mut capacity_probe =
                tokio::spawn(async move { capacity_lock.get_write_lock(std::time::Duration::from_secs(30)).await });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(50), &mut capacity_probe)
                    .await
                    .is_err(),
                "early-ACK tail must retain the decommission capacity guard"
            );
            capacity_probe.abort();
            let _ = capacity_probe.await;

            tail_barrier.release();
            drop(tail_barrier);
            let object_guard = tokio::time::timeout(std::time::Duration::from_secs(30), object_probe)
                .await
                .expect("object guard probe should finish after the tail")
                .expect("object guard probe should join")
                .expect("object guard probe should acquire after the tail");
            drop(object_guard);
            let capacity_lock = other_store
                .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
                .await
                .expect("create the post-tail decommission capacity guard probe");
            let capacity_guard = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                capacity_lock.get_write_lock(std::time::Duration::from_secs(30)),
            )
            .await
            .expect("capacity guard probe should finish after the tail")
            .expect("capacity guard probe should acquire after the tail");
            drop(capacity_guard);
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut ordinary_mutation)
                .await
                .expect("restore should return its early quorum acknowledgement")
                .expect("restore task should join")
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(30), ordinary_mutation)
                .await
                .expect("ordinary object mutation must finish after the migration releases its object fence")
                .expect("ordinary object mutation task should join")
        };
        drop(barrier);
        ordinary_result.unwrap_or_else(|error| {
            panic!(
                "ordinary object mutation should commit to the unreserved target: {error:?}; \
                 ordinary_start_after_launch={:?}, lock_phase_observed_after_launch={:?}, \
                 owner_release_after_launch={:?}, migration_after_owner_release={:?}, \
                 ordinary_finish_after_launch={:?}, ordinary_finished_before_migration={}",
                ordinary_started_at.duration_since(ordinary_launched_at),
                ordinary_lock_phase_observed_at.duration_since(ordinary_launched_at),
                owner_released_at.duration_since(ordinary_launched_at),
                migration_finished_at.duration_since(owner_released_at),
                ordinary_finished_at.duration_since(ordinary_launched_at),
                ordinary_finished_at < migration_finished_at,
            );
        });
        store.pools[1]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version),
                    ..Default::default()
                },
            )
            .await
            .expect("migration should preserve the source version on the reserved target");
        store.pools[2]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("ordinary object mutation should stay on the unreserved target");
        if matches!(mutation, ExternalObjectMutation::Restore) {
            let mut restored = store.pools[2]
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("restored target should be readable");
            let mut restored_body = Vec::new();
            restored
                .stream
                .read_to_end(&mut restored_body)
                .await
                .expect("restored target body should be readable");
            assert_eq!(restored_body, b"existing unreserved target");
        }
    }

    async fn assert_same_object_historical_copy_lock_order() {
        let (_temp_dirs, store, other_store) = test_three_pool_stores_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("same-object-copy-order");
        let object = "same-object.bin";
        let migration_version = uuid::Uuid::new_v4().to_string();
        let historical_version = uuid::Uuid::new_v4().to_string();
        let current_version = uuid::Uuid::new_v4().to_string();
        let migration_body = b"decommission migration body".to_vec();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create same-object copy lock-order bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load same-object copy bucket incarnation");

        let mut migration_source = PutObjReader::from_vec(migration_body.clone());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut migration_source,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(migration_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the migration source version");

        let mut historical_source = PutObjReader::from_vec(b"historical copy body".to_vec());
        store.pools[2]
            .put_object(
                &bucket,
                object,
                &mut historical_source,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(historical_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the historical copy version");
        let mut current_source = PutObjReader::from_vec(b"current copy body".to_vec());
        store.pools[2]
            .put_object(
                &bucket,
                object,
                &mut current_source,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(current_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed the current copy version");

        let mut copy_info = store.pools[2]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    version_id: Some(historical_version.clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("historical copy version should be readable");
        assert_eq!(
            store.pools[2]
                .get_object_info(
                    &bucket,
                    object,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("current copy version should be readable")
                .version_id
                .map(|version| version.to_string()),
            Some(current_version.clone())
        );

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = migration_body.len() * 4;
        let capacity_snapshot = |target_free| {
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, migration_body.len(), migration_body.len()),
                DecommissionPoolCapacityInfo::for_test(
                    1,
                    layout,
                    target_free,
                    target_total,
                    target_total.saturating_sub(target_free),
                ),
                DecommissionPoolCapacityInfo::for_test(2, layout, 0, target_total, target_total),
            ]
        };
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity_snapshot(target_total)]);
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the same-object copy source reservation");
        *other_store.pool_meta.write().await = store.pool_meta.read().await.clone();
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);

        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![
                capacity_snapshot(target_total),
                capacity_snapshot(target_total),
                capacity_snapshot(target_total - migration_body.len()),
            ],
        );
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(migration_version),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the migration source version");
        let barrier = DecommissionCapacityLockOrderBarrier::install(store.id, other_store.id);
        let migration = tokio::spawn({
            let migration_store = Arc::clone(&store);
            let migration_bucket = bucket.clone();
            async move {
                let result = data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "same_object_copy_lock_order",
                    Some(owner),
                )
                .await;
                (result, std::time::Instant::now())
            }
        });
        barrier.wait_until_owner_paused().await;

        let ordinary_launched_at = std::time::Instant::now();
        let copy_task = tokio::spawn({
            let copy_store = Arc::clone(&other_store);
            let copy_bucket = bucket.clone();
            let source_opts = ObjectOptions {
                versioned: true,
                version_id: Some(historical_version),
                ..Default::default()
            };
            let destination_opts = ObjectOptions {
                expected_current_version_id: Some(current_version),
                versioned: true,
                ..Default::default()
            };
            async move {
                let ordinary_started_at = std::time::Instant::now();
                let result = copy_store
                    .copy_object(
                        &copy_bucket,
                        object,
                        &copy_bucket,
                        object,
                        &mut copy_info,
                        &source_opts,
                        &destination_opts,
                    )
                    .await
                    .map(|_| ());
                (result, ordinary_started_at, std::time::Instant::now())
            }
        });
        barrier.wait_until_external_capacity_released().await;
        let ordinary_lock_phase_observed_at = std::time::Instant::now();
        assert!(
            !copy_task.is_finished(),
            "historical same-object CopyObject must wait behind the migration object fence"
        );
        let owner_released_at = std::time::Instant::now();
        barrier.release_owner();
        drop(barrier);

        let (migration_result, migration_finished_at) = tokio::time::timeout(std::time::Duration::from_secs(30), migration)
            .await
            .expect("migration must not deadlock with historical same-object CopyObject")
            .expect("migration task should join");
        migration_result.expect("migration should commit to its reserved target");
        let (ordinary_result, ordinary_started_at, ordinary_finished_at) =
            tokio::time::timeout(std::time::Duration::from_secs(30), copy_task)
                .await
                .expect("historical same-object CopyObject must finish after the migration releases its object fence")
                .expect("CopyObject task should join");
        ordinary_result.unwrap_or_else(|error| {
            panic!(
                "CopyObject should preserve the expected current-version precondition: {error:?}; \
                 ordinary_start_after_launch={:?}, lock_phase_observed_after_launch={:?}, \
                 owner_release_after_launch={:?}, migration_after_owner_release={:?}, \
                 ordinary_finish_after_launch={:?}, ordinary_finished_before_migration={}",
                ordinary_started_at.duration_since(ordinary_launched_at),
                ordinary_lock_phase_observed_at.duration_since(ordinary_launched_at),
                owner_released_at.duration_since(ordinary_launched_at),
                migration_finished_at.duration_since(owner_released_at),
                ordinary_finished_at.duration_since(ordinary_launched_at),
                ordinary_finished_at < migration_finished_at,
            );
        });
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_unreserved_put_locks() {
        assert_lock_order(ExternalObjectMutation::Put).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_multipart_complete_locks() {
        assert_lock_order(ExternalObjectMutation::CompleteMultipart).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_cross_object_copy_locks() {
        assert_lock_order(ExternalObjectMutation::Copy).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn public_cross_object_copy_holds_decommission_capacity_through_early_ack_tail() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            assert_lock_order_with_tail(ExternalObjectMutation::Copy, true).await
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_same_object_historical_copy_locks() {
        assert_same_object_historical_copy_lock_order().await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_put_object_tag_locks() {
        assert_lock_order(ExternalObjectMutation::PutTags).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_delete_object_tag_locks() {
        assert_lock_order(ExternalObjectMutation::DeleteTags).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_put_object_metadata_locks() {
        assert_lock_order(ExternalObjectMutation::PutMetadata).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_delete_object_locks() {
        assert_lock_order(ExternalObjectMutation::Delete).await;
    }

    #[test]
    #[serial_test::serial]
    fn active_decommission_migration_does_not_invert_capacity_and_restore_locks() {
        run_large_stack_async_test("decommission-restore-lock-order", || async {
            temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
                assert_lock_order_with_tail(ExternalObjectMutation::Restore, true).await
            })
            .await;
        });
    }

    async fn killed_leader_target(
        store: &Arc<crate::store::ECStore>,
        target_pool_index: usize,
    ) -> crate::core::pools::DecommissionCapacityTarget {
        store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .and_then(|reservation| {
                reservation
                    .targets
                    .iter()
                    .find(|target| target.pool_index == target_pool_index)
            })
            .cloned()
            .expect("the reservation should allocate the target pool")
    }

    async fn run_resumed_decommission_routine(store: &Arc<crate::store::ECStore>) {
        let canceler = crate::core::pools::DecommissionCanceler::new(CancellationToken::new());
        store.decommission_cancelers.write().await[0] = Some(canceler.clone());
        store
            .do_decommission_in_routine(canceler, 0, Arc::new(tokio::sync::Semaphore::new(1)))
            .await
            .expect("the resumed decommission routine should return");
    }

    /// Kill the decommission leader inside the target write window of a real
    /// single-PUT migration: before the target commit, or after it but before
    /// the capacity finalize. Returns the bucket and the interrupted mutation.
    async fn kill_leader_inside_target_put(
        store: &Arc<crate::store::ECStore>,
        committed: bool,
        object: &str,
    ) -> (String, uuid::Uuid) {
        // The routine also migrates `.rustfs.sys` bucket metadata, so keep the
        // reservation far above every fixture object.
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        kill_leader_inside_target_put_with_capacity(
            store,
            committed,
            object,
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                DecommissionPoolCapacityInfo::for_test(1, layout, 1 << 30, 1 << 30, 0),
            ],
        )
        .await
    }

    /// Like `kill_leader_inside_target_put`, with the capacity snapshot the
    /// decommission reserves from. The selection must route the killed
    /// migration to target pool 1.
    async fn kill_leader_inside_target_put_with_capacity(
        store: &Arc<crate::store::ECStore>,
        committed: bool,
        object: &str,
        capacity: Vec<DecommissionPoolCapacityInfo>,
    ) -> (String, uuid::Uuid) {
        let bucket = test_bucket("killed-leader-put");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the killed-leader bucket");
        let mut source = PutObjReader::from_vec(b"source survives the leader kill".to_vec());
        store.pools[0]
            .put_object(&bucket, object, &mut source, &ObjectOptions::default())
            .await
            .expect("write the source object before decommission starts");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the killed-leader bucket incarnation");
        set_decommission_capacity_info_overrides_for_test(store.id, vec![capacity]);
        store
            .save_current_pool_meta_for_decommission_start(
                &[0],
                vec![crate::core::pools::DecomBucketInfo {
                    name: bucket.clone(),
                    prefix: String::new(),
                }],
            )
            .await
            .expect("activate the killed-leader decommission");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the source object for the interrupted migration");
        let pause = if committed {
            PutObjectCommitPause::AfterRenameHandoff
        } else {
            PutObjectCommitPause::BeforeQuotaRename
        };
        let barrier = PutObjectCommitBarrier::install(&bucket, object, pause);
        let mut migration = tokio::spawn({
            let migration_store = Arc::clone(store);
            let migration_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "killed_leader_put",
                    Some(owner),
                )
                .await
            }
        });
        tokio::select! {
            () = barrier.wait_until_paused() => {}
            result = &mut migration => {
                panic!("the migration finished before the leader could be killed in its target write window: {result:?}");
            }
        }
        // The leader dies inside the target write window.
        migration.abort();
        let _ = migration.await;
        barrier.release();
        drop(barrier);

        let interrupted = killed_leader_target(store, 1)
            .await
            .pending_mutation_id
            .expect("the killed mutation must leave its durable capacity intent");
        let target_copy = store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await;
        assert_eq!(
            target_copy.is_ok(),
            committed,
            "the kill window must match the requested commit state: {target_copy:?}"
        );
        (bucket, interrupted)
    }

    async fn assert_resumed_decommission_settled(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        interrupted: uuid::Uuid,
    ) {
        let local = store.pool_meta.read().await;
        let info = local.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert!(
            info.complete && !info.failed && !info.canceled,
            "the resumed decommission must complete: complete={}, failed={}, canceled={}, blocked_reason={:?}",
            info.complete,
            info.failed,
            info.canceled,
            info.capacity_blocked_reason
        );
        assert_eq!(info.items_decommission_failed, 0);
        drop(local);
        let target = killed_leader_target(store, 1).await;
        assert_eq!(target.pending_mutation_id, None);
        assert_eq!(target.pending_physical_bytes, 0);
        let adopted = crate::core::pools::decommission_adopted_intent_mutation_id(interrupted);
        assert!(
            target
                .temporary_mutations
                .iter()
                .all(|mutation| mutation.mutation_id != interrupted && mutation.mutation_id != adopted),
            "the interrupted mutation's retry must settle its markers: {:?}",
            target.temporary_mutations
        );
        assert!(target.consumed_physical_bytes > 0);
        store.pools[1]
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("the resumed decommission should migrate the source object");
        let source = store.pools[0]
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await;
        assert!(
            source
                .as_ref()
                .err()
                .is_some_and(|err| crate::error::is_err_object_not_found(err) || crate::error::is_err_version_not_found(err)),
            "the source copy should be cleaned after migration: {source:?}"
        );
    }

    async fn resumed_decommission_settles_a_killed_leader_put_case(committed: bool, legacy: bool) {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        let _proof_guard = legacy.then(crate::services::notification_sys::without_decommission_target_fence_fleet_proof_for_test);
        let object = "killed-leader.bin";
        let (bucket, interrupted) = kill_leader_inside_target_put(&store, committed, object).await;

        // The restarted leader resumes the same decommission. Its retry of the
        // interrupted object either finds the committed copy and reconciles it
        // or rewrites it, and either way settles the adopted marker.
        run_resumed_decommission_routine(&store).await;
        assert_resumed_decommission_settled(&store, &bucket, object, interrupted).await;
    }

    #[test]
    #[serial_test::serial]
    fn resumed_decommission_settles_a_leader_killed_after_the_target_commit() {
        run_large_stack_async_test("resumed-decommission-killed-after-commit", || {
            resumed_decommission_settles_a_killed_leader_put_case(true, false)
        });
    }

    #[test]
    #[serial_test::serial]
    fn resumed_decommission_settles_a_leader_killed_before_the_target_commit() {
        run_large_stack_async_test("resumed-decommission-killed-before-commit", || {
            resumed_decommission_settles_a_killed_leader_put_case(false, false)
        });
    }

    #[test]
    #[serial_test::serial]
    fn resumed_legacy_decommission_settles_a_leader_killed_after_the_target_commit() {
        run_large_stack_async_test("resumed-legacy-decommission-killed-after-commit", || {
            resumed_decommission_settles_a_killed_leader_put_case(true, true)
        });
    }

    #[test]
    #[serial_test::serial]
    fn resumed_legacy_decommission_settles_a_leader_killed_before_the_target_commit() {
        run_large_stack_async_test("resumed-legacy-decommission-killed-before-commit", || {
            resumed_decommission_settles_a_killed_leader_put_case(false, true)
        });
    }

    /// Move part of one target's remaining allocation to another target and
    /// persist it, so that the next selection by remaining allocation alone
    /// would prefer `to`.
    async fn shift_decommission_target_allocation(store: &Arc<crate::store::ECStore>, from: usize, to: usize, bytes: usize) {
        let mut meta = store.pool_meta.read().await.clone();
        let reservation = meta.pools[0]
            .decommission
            .as_mut()
            .and_then(|info| info.capacity_reservation.as_mut())
            .expect("the decommission reservation should be active");
        for target in &mut reservation.targets {
            if target.pool_index == from {
                target.reserved_physical_bytes -= bytes;
            } else if target.pool_index == to {
                target.reserved_physical_bytes += bytes;
            }
        }
        meta.save(store.pools.clone())
            .await
            .expect("persist the shifted target allocation");
        *store.pool_meta.write().await = meta;
    }

    async fn resumed_decommission_returns_an_uncommitted_retry_to_its_adopted_target_case(legacy: bool) {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_three_pool_stores(None).await;
        let _proof_guard = legacy.then(crate::services::notification_sys::without_decommission_target_fence_fleet_proof_for_test);
        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        // Target 1 holds most of the allocation when the leader is killed
        // before its target commit there.
        let object = "sticky-adopted-target.bin";
        let (bucket, interrupted) = kill_leader_inside_target_put_with_capacity(
            &store,
            false,
            object,
            vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, 64 << 20, 64 << 20),
                DecommissionPoolCapacityInfo::for_test(1, layout, 100 << 20, 100 << 20, 0),
                DecommissionPoolCapacityInfo::for_test(2, layout, 1 << 30, 1 << 30, 0),
            ],
        )
        .await;
        // Before the resume, target 2 has the larger remaining allocation.
        shift_decommission_target_allocation(&store, 1, 2, 80 << 20).await;
        assert!(
            killed_leader_target(&store, 2).await.reserved_physical_bytes
                > killed_leader_target(&store, 1).await.reserved_physical_bytes
        );

        run_resumed_decommission_routine(&store).await;
        let local = store.pool_meta.read().await;
        let info = local.pools[0]
            .decommission
            .as_ref()
            .expect("the decommission state should remain present");
        assert!(
            info.complete && !info.failed && !info.canceled,
            "the resumed decommission must complete: complete={}, failed={}, canceled={}, blocked_reason={:?}",
            info.complete,
            info.failed,
            info.canceled,
            info.capacity_blocked_reason
        );
        assert_eq!(info.items_decommission_failed, 0);
        drop(local);
        let adopted = crate::core::pools::decommission_adopted_intent_mutation_id(interrupted);
        for target_pool_index in [1, 2] {
            let target = killed_leader_target(&store, target_pool_index).await;
            assert_ne!(target.pending_mutation_id, Some(interrupted));
            assert!(
                target
                    .temporary_mutations
                    .iter()
                    .all(|mutation| mutation.mutation_id != interrupted && mutation.mutation_id != adopted),
                "target pool {target_pool_index} must not keep a marker of the interrupted mutation: {:?}",
                target.temporary_mutations
            );
        }
        assert!(
            killed_leader_target(&store, 1).await.consumed_physical_bytes > 0,
            "the retry's commit must be charged on the target that held its adopted completion"
        );
        store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("the retry must commit on the target that holds its adopted completion");
        let source = store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await;
        assert!(
            source
                .as_ref()
                .err()
                .is_some_and(|err| crate::error::is_err_object_not_found(err) || crate::error::is_err_version_not_found(err)),
            "the source copy should be cleaned after migration: {source:?}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn resumed_decommission_returns_an_uncommitted_retry_to_its_adopted_target() {
        run_large_stack_async_test("resumed-decommission-sticky-adopted-target", || {
            resumed_decommission_returns_an_uncommitted_retry_to_its_adopted_target_case(false)
        });
    }

    #[test]
    #[serial_test::serial]
    fn resumed_legacy_decommission_returns_an_uncommitted_retry_to_its_adopted_target() {
        run_large_stack_async_test("resumed-legacy-decommission-sticky-adopted-target", || {
            resumed_decommission_returns_an_uncommitted_retry_to_its_adopted_target_case(true)
        });
    }

    #[test]
    #[serial_test::serial]
    fn failed_intent_adoption_pauses_and_the_next_incarnation_completes() {
        run_large_stack_async_test("failed-intent-adoption-pauses", || async {
            let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
            let object = "adoption-failure.bin";
            let (bucket, interrupted) = kill_leader_inside_target_put(&store, true, object).await;

            crate::core::pools::fail_next_decommission_intent_adoption_for_test(store.id);
            run_resumed_decommission_routine(&store).await;
            let local = store.pool_meta.read().await;
            let info = local.pools[0]
                .decommission
                .as_ref()
                .expect("the decommission state should remain present");
            assert!(
                !info.complete && !info.failed && !info.canceled,
                "an adoption failure must pause the routine, not end the decommission: complete={}, failed={}, canceled={}",
                info.complete,
                info.failed,
                info.canceled
            );
            assert!(
                info.capacity_blocked_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("failed to adopt target capacity intents")),
                "the pause must name the adoption failure: {:?}",
                info.capacity_blocked_reason
            );
            drop(local);
            assert_eq!(killed_leader_target(&store, 1).await.pending_mutation_id, Some(interrupted));

            run_resumed_decommission_routine(&store).await;
            assert_resumed_decommission_settled(&store, &bucket, object, interrupted).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn adopted_multipart_completion_survives_a_background_abort_until_reconciled() {
        run_large_stack_async_test(
            "adopted-multipart-completion-background-abort",
            adopted_multipart_completion_survives_a_background_abort_until_reconciled_case,
        );
    }

    async fn adopted_multipart_completion_survives_a_background_abort_until_reconciled_case() {
        let (_temp_dirs, store, _other_store) =
            test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(None).await;
        let bucket = test_bucket("adopted-mpu");
        let object = "adopted-multipart-completion.bin";
        let first_part = vec![0x61; 5 * 1024 * 1024];
        let second_part = vec![0x62; 1];
        let body_len = first_part.len() + second_part.len();
        let source_version = uuid::Uuid::new_v4().to_string();
        let source_mod_time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(43);

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create the adopted multipart bucket");
        let incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("load the adopted multipart bucket incarnation");
        let source_opts = ObjectOptions {
            versioned: true,
            version_id: Some(source_version.clone()),
            mod_time: Some(source_mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let source_upload = new_multipart_upload(&store, 0, &bucket, object, source_opts.clone())
            .await
            .expect("create the source multipart upload");
        let source_multipart_opts = multipart_options(&store, &bucket, source_opts).await;
        let mut source_parts = Vec::with_capacity(2);
        for (part_num, part_body) in [(1, first_part.as_slice()), (2, second_part.as_slice())] {
            let mut part_reader = PutObjReader::from_vec(part_body.to_vec());
            let part = store.pools[0]
                .put_object_part(
                    &bucket,
                    object,
                    &source_upload.upload_id,
                    part_num,
                    &mut part_reader,
                    &source_multipart_opts,
                )
                .await
                .expect("stage the source multipart part");
            source_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        let source_info = store.pools[0]
            .clone()
            .complete_multipart_upload(&bucket, object, &source_upload.upload_id, source_parts, &source_multipart_opts)
            .await
            .expect("complete the source multipart object");
        assert!(source_info.is_multipart(), "the source must take the multipart migration path");

        let layout = DecommissionErasureLayout { data: 1, parity: 0 };
        let target_total = body_len.saturating_mul(4);
        set_decommission_capacity_info_overrides_for_test(
            store.id,
            vec![vec![
                DecommissionPoolCapacityInfo::for_test(0, layout, 0, body_len, body_len),
                DecommissionPoolCapacityInfo::for_test(1, layout, 0, target_total, target_total),
                DecommissionPoolCapacityInfo::for_test(2, layout, target_total, target_total, 0),
            ]],
        );
        store
            .save_current_pool_meta_for_decommission_start(&[0], Vec::new())
            .await
            .expect("activate the multipart capacity reservation");
        let owner = decommission_capacity_owner(&*store.pool_meta.read().await);
        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.clone()),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read the source multipart object for migration");

        // The leader dies after the completion published the target and before
        // the completion's capacity finalize.
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::AfterObjectPublication);
        let mut migration = tokio::spawn({
            let migration_store = Arc::clone(&store);
            let migration_bucket = bucket.clone();
            async move {
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "adopted_multipart_completion",
                    Some(owner),
                )
                .await
            }
        });
        tokio::select! {
            () = barrier.wait_until_paused() => {}
            result = &mut migration => {
                panic!("the multipart migration finished before its completion could be interrupted: {result:?}");
            }
        }
        migration.abort();
        let _ = migration.await;
        barrier.release();
        drop(barrier);

        let interrupted = decommission_capacity_mutation_id(
            owner,
            &bucket,
            object,
            Some(source_version.as_str()),
            false,
            Some(source_mod_time),
        );
        let leftover = killed_leader_target(&store, 2).await;
        assert_eq!(leftover.pending_mutation_id, Some(interrupted));
        let completion_bytes = leftover.pending_physical_bytes;
        assert!(completion_bytes > 0);

        store
            .adopt_previous_incarnation_decommission_capacity_intents_for_test(0)
            .await
            .expect("the resumed routine should adopt the interrupted completion");
        let adopted = crate::core::pools::decommission_adopted_intent_mutation_id(interrupted);
        let adopted_marker = killed_leader_target(&store, 2)
            .await
            .temporary_mutations
            .iter()
            .find(|mutation| mutation.mutation_id == adopted)
            .map(|mutation| mutation.physical_bytes)
            .expect("the interrupted completion should become its adopted-completion marker");
        assert!(adopted_marker >= completion_bytes);

        // The failed attempt's cleanup runs after the resume and finds the
        // published target instead of a staged upload.
        let mut cleanup_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(source_version.clone()),
            mod_time: Some(source_mod_time),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        let interrupted_owner = owner.with_mutation_id(interrupted);
        interrupted_owner.apply_to(&mut cleanup_opts);
        let multipart_fence = store
            .acquire_decommission_multipart_mutation_fence(interrupted_owner)
            .await
            .expect("fence the background multipart cleanup");
        multipart_fence.add_namespace_lock_fence(&mut cleanup_opts);
        store
            .reconcile_multipart_uploads_for_data_movement(
                2,
                &bucket,
                object,
                &data_movement::data_movement_upload_identity_from_options(&cleanup_opts),
                &cleanup_opts,
            )
            .await
            .expect("the background cleanup should prove the published target");
        drop(multipart_fence);
        assert_eq!(
            killed_leader_target(&store, 2)
                .await
                .temporary_mutations
                .iter()
                .find(|mutation| mutation.mutation_id == adopted)
                .map(|mutation| mutation.physical_bytes),
            Some(adopted_marker),
            "a cleanup that proves the committed target must not release the adopted completion"
        );

        // The interrupted mutation's retry finds the published target.
        store
            .reconcile_decommission_capacity_after_equivalent_target(interrupted_owner, 2, body_len)
            .await
            .expect("the equivalent target settles the adopted completion");
        let settled = killed_leader_target(&store, 2).await;
        assert_eq!(settled.consumed_physical_bytes, completion_bytes);
        assert!(
            settled
                .temporary_mutations
                .iter()
                .all(|mutation| mutation.mutation_id != interrupted && mutation.mutation_id != adopted)
        );
        assert_eq!(settled.inflight_physical_bytes, 0);
    }
}
