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
        DecommissionCapacityLockOrderBarrier, DecommissionCapacityOwner, DecommissionErasureLayout, DecommissionPoolCapacityInfo,
        POOL_META_NAME, set_decommission_capacity_info_overrides_for_test,
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
    use std::collections::HashMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

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
        target: rustfs_lock::ObjectKey,
        control: Arc<CapacityLeaseLossControl>,
        active: tokio::sync::Mutex<HashMap<rustfs_lock::LockId, rustfs_lock::ObjectKey>>,
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
            if resource.as_ref() == Some(&self.target) {
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
                    target: rustfs_lock::ObjectKey::new(RUSTFS_META_BUCKET, POOL_META_NAME),
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
        *ctx.local_disk_map().write().await = other_store.ctx.local_disk_map().read().await.clone();
        let endpoints = other_store
            .ctx
            .endpoints()
            .expect("lease-loss test store should have endpoint topology");
        ctx.set_endpoints(endpoints.clone());
        if let Some(deployment_id) = other_store.ctx.deployment_id() {
            ctx.set_deployment_id(deployment_id);
        }
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
            decommission_capacity_entry_gate: tokio::sync::Mutex::default(),
            ctx,
            bucket_fence_registry: Arc::default(),
        });
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        (store, refresh_calls)
    }

    async fn assert_lock_order(mutation: ExternalObjectMutation) {
        assert_lock_order_with_tail(mutation, false).await;
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
        tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            probe_store
                .save_current_pool_meta_for_test(&[0])
                .await
                .expect("pool metadata mutation probe should commit before UploadPart admission");
        })
        .await
        .expect("public UploadPart must not hold a capacity read guard during staging");

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

            tail_barrier.release();
            drop(tail_barrier);
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

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_restart_reconciles_published_capacity_before_new_upload() {
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
                    "multipart_restart_reconcile_initial",
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
            .expect("the multipart target must publish before capacity progress save");
        assert!(published.is_multipart(), "published target must retain multipart identity");
        assert_eq!(published.version_id.map(|version| version.to_string()), Some(source_version.clone()));
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
        assert!(failed_reservation.pending_target_physical_bytes > 0);
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

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_restart_cleans_partial_upload_before_exact_fit_retry() {
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
        let retry = tokio::spawn({
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
        retry_barrier.wait_until_paused().await;
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
        let mut upload_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        rustfs_utils::http::insert_str(
            &mut upload_opts.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            "v1:abort-fence:1".to_string(),
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

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_abort_restart_reconciles_inflight_after_release_save_loss() {
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
        let upload_identity = format!("v1:{}:{}", uuid::Uuid::new_v4(), 91);
        let mut new_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
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
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        owner.apply_to(&mut abort_opts);
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::AbortAfterDelete);
        tokio::time::pause();
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

        let capacity_lock = other_store
            .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
            .await
            .expect("create the data-movement tail capacity probe");
        let mut capacity_probe =
            tokio::spawn(async move { capacity_lock.get_write_lock(std::time::Duration::from_secs(30)).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut capacity_probe)
                .await
                .is_err(),
            "data-movement completion must retain the capacity write lock while its tail is paused"
        );

        tail_barrier.release();
        drop(tail_barrier);
        tokio::time::timeout(std::time::Duration::from_secs(30), complete)
            .await
            .expect("data-movement CompleteMultipartUpload should finish after its tail resumes")
            .expect("data-movement CompleteMultipartUpload task should not panic")
            .expect("data-movement CompleteMultipartUpload should commit after its tail resumes");
        let capacity_guard = tokio::time::timeout(std::time::Duration::from_secs(30), capacity_probe)
            .await
            .expect("capacity probe should finish after data-movement tail completion")
            .expect("capacity probe should not panic")
            .expect("capacity probe should acquire after data-movement tail completion");
        drop(capacity_guard);

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
            crate::error::is_err_object_not_found(&object_err),
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
                    version_id: Some(next_version_id),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the next mutation must remain unpublished after A recovery");
        assert!(crate::error::is_err_object_not_found(&next_target_err));
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
        let capacity_lock = other_store
            .new_ns_lock(RUSTFS_META_BUCKET, POOL_META_NAME)
            .await
            .expect("create the data-movement PUT tail capacity probe");
        let mut capacity_probe = tokio::spawn(async move { capacity_lock.get_write_lock(Duration::from_secs(30)).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut capacity_probe)
                .await
                .is_err(),
            "data-movement PUT tail must retain its capacity fence"
        );
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

        let capacity_guard = tokio::time::timeout(Duration::from_secs(30), capacity_probe)
            .await
            .expect("capacity probe should finish after the data-movement PUT tail")
            .expect("capacity probe should not panic")
            .expect("capacity probe should acquire after the data-movement PUT tail");
        drop(capacity_guard);

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
    async fn data_movement_equivalent_target_reconciles_published_capacity_after_restart() {
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
        let retry_reader = other_store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version),
                    no_lock: true,
                    data_movement: true,
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("restart retry should reread the exact source version");
        tokio::time::timeout(
            Duration::from_secs(30),
            data_movement::migrate_decommission_object(
                Arc::clone(&other_store),
                0,
                bucket.clone(),
                retry_reader,
                Some(incarnation),
                "equivalent_target_reconcile_retry",
                Some(retry_owner),
            ),
        )
        .await
        .expect("equivalent-target retry should not deadlock")
        .expect("equivalent target retry should reconcile the pending capacity intent");

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
            let backend = register_mock_tier(&store.pools[2].instance_ctx().tier_config_mgr(), &tier_name).await;
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
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "decommission_object_lock_order",
                    Some(owner),
                )
                .await
            }
        });
        barrier.wait_until_owner_paused().await;

        let mut ordinary_mutation = tokio::spawn({
            let ordinary_store = Arc::clone(&other_store);
            let ordinary_bucket = bucket.clone();
            async move {
                match mutation {
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
                }
            }
        });
        if let Some(get_barrier) = restore_get_barrier.as_ref() {
            get_barrier.wait_until_paused().await;
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
        assert!(!ordinary_mutation.is_finished());
        let migration_result = if pause_restore_tail {
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

        let ordinary_result = if pause_restore_tail {
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

            tail_barrier.release();
            drop(tail_barrier);
            let object_guard = tokio::time::timeout(std::time::Duration::from_secs(30), object_probe)
                .await
                .expect("object guard probe should finish after the tail")
                .expect("object guard probe should join")
                .expect("object guard probe should acquire after the tail");
            drop(object_guard);
            let capacity_guard = tokio::time::timeout(std::time::Duration::from_secs(30), capacity_probe)
                .await
                .expect("capacity guard probe should finish after the tail")
                .expect("capacity guard probe should join")
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
        ordinary_result.expect("ordinary object mutation should commit to the unreserved target");
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
                data_movement::migrate_decommission_object(
                    migration_store,
                    0,
                    migration_bucket,
                    source_reader,
                    Some(incarnation),
                    "same_object_copy_lock_order",
                    Some(owner),
                )
                .await
            }
        });
        barrier.wait_until_owner_paused().await;

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
                    .map(|_| ())
            }
        });
        barrier.wait_until_external_capacity_released().await;
        assert!(
            !copy_task.is_finished(),
            "historical same-object CopyObject must wait behind the migration object fence"
        );
        barrier.release_owner();
        drop(barrier);

        tokio::time::timeout(std::time::Duration::from_secs(30), migration)
            .await
            .expect("migration must not deadlock with historical same-object CopyObject")
            .expect("migration task should join")
            .expect("migration should commit to its reserved target");
        tokio::time::timeout(std::time::Duration::from_secs(30), copy_task)
            .await
            .expect("historical same-object CopyObject must finish after the migration releases its object fence")
            .expect("CopyObject task should join")
            .expect("CopyObject should preserve the expected current-version precondition");
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

    #[tokio::test]
    #[serial_test::serial]
    async fn active_decommission_migration_does_not_invert_capacity_and_restore_locks() {
        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            assert_lock_order_with_tail(ExternalObjectMutation::Restore, true).await
        })
        .await;
    }
}
