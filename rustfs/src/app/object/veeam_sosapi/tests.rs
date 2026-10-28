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

fn empty_member_archive(key: &str) -> Bytes {
    let mut header = tokio_tar::Header::new_gnu();
    header.set_path(key).expect("archive member path");
    header.set_size(0);
    header.set_mode(0o644);
    header.set_cksum();
    let mut archive = header.as_bytes().to_vec();
    archive.extend_from_slice(&[0; 1024]);
    Bytes::from(archive)
}

fn element_text<'a>(xml: &'a str, element: &str) -> &'a str {
    let start = format!("<{element}>");
    let end = format!("</{element}>");
    let start_idx = xml.find(&start).unwrap_or_else(|| panic!("missing {element}")) + start.len();
    let rest = &xml[start_idx..];
    let end_idx = rest.find(&end).unwrap_or_else(|| panic!("unclosed {element}"));
    &rest[..end_idx]
}

#[test]
fn matches_only_the_exact_sosapi_keys() {
    assert_eq!(match_sosapi_key(SYSTEM_KEY), Some(SosapiKind::System));
    assert_eq!(match_sosapi_key(CAPACITY_KEY), Some(SosapiKind::Capacity));
    assert_eq!(match_sosapi_key(&format!("{SYSTEM_KEY}/")), None);
    assert_eq!(match_sosapi_key(".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/"), None);
    assert_eq!(match_sosapi_key("Veeam/Backup/job.dat"), None);
    assert_eq!(match_sosapi_key(&format!("prefix/{SYSTEM_KEY}")), None);
}

#[test]
#[serial_test::serial]
fn flag_off_leaves_the_keys_unhandled() {
    temp_env::with_var(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"), || {
        assert!(handled_kind(SYSTEM_KEY).is_none());
        assert!(!mutation_blocked(CAPACITY_KEY));
    });
    temp_env::with_var_unset(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, || {
        assert_eq!(handled_kind(SYSTEM_KEY), Some(SosapiKind::System));
        assert!(mutation_blocked(CAPACITY_KEY));
    });
}

#[test]
fn system_xml_keeps_minio_quote_spelling_and_omits_optional_blocks() {
    let xml = system_xml("1.0.2");
    assert_eq!(element_text(&xml, "ProtocolVersion"), "\"1.0\"");
    assert_eq!(element_text(&xml, "ModelName"), "\"RustFS 1.0.2\"");
    assert_eq!(element_text(&xml, "CapacityInfo"), "true");
    assert_eq!(element_text(&xml, "UploadSessions"), "false");
    assert_eq!(element_text(&xml, "IAMSTS"), "false");
    assert!(xml.contains("<ProtocolCapabilities>"));
    assert!(!xml.contains("SystemRecommendations"));
    assert!(!xml.contains("APIEndpoints"));
    assert!(!xml.contains("&quot;"));
    let escaped = system_xml("1<2>&");
    assert!(escaped.contains("\"RustFS 1&lt;2&gt;&amp;\""));
}

#[test]
fn capacity_math_clamps_used_and_available() {
    let quota = capacity_figures(CapacityInputs {
        used_bucket: 250,
        hard_quota: Some(1_000),
        capacity_cluster: 9_000,
        free_cluster: 8_000,
    });
    assert_eq!(
        quota,
        CapacityFigures {
            capacity: 1_000,
            used: 250,
            available: 750,
        }
    );

    let cluster = capacity_figures(CapacityInputs {
        used_bucket: 100,
        hard_quota: None,
        capacity_cluster: 5_000,
        free_cluster: 4_000,
    });
    assert_eq!(cluster.capacity, 5_000);
    assert_eq!(cluster.used, 100);
    assert_eq!(cluster.available, 4_000);

    let saturated = capacity_figures(CapacityInputs {
        used_bucket: 9_000,
        hard_quota: Some(1_000),
        capacity_cluster: 50_000,
        free_cluster: 40_000,
    });
    assert_eq!(saturated.used, 1_000);
    assert_eq!(saturated.available, 0);

    let free_clamp = capacity_figures(CapacityInputs {
        used_bucket: 100,
        hard_quota: Some(10_000),
        capacity_cluster: 50_000,
        free_cluster: 50,
    });
    assert_eq!(free_clamp.available, 50);
    assert!(free_clamp.available <= free_clamp.capacity - free_clamp.used);

    let zero_quota = capacity_figures(CapacityInputs {
        used_bucket: 10,
        hard_quota: Some(0),
        capacity_cluster: 400,
        free_cluster: 300,
    });
    assert_eq!(zero_quota.capacity, 400);
}

#[tokio::test]
async fn concurrent_capacity_refreshes_share_one_load_and_preserve_modification_time() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let caches = CapacityCaches::new();
    let key = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        bucket: "bucket".into(),
        incarnation: Uuid::new_v4(),
    };
    let calls = AtomicUsize::new(0);
    let figures = || CapacityFigures {
        capacity: 100,
        used: 20,
        available: 80,
    };
    let load = || async {
        calls.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok(figures())
    };
    let (first, second) =
        tokio::join!(cached_or_load_capacity(&caches, &key, load), cached_or_load_capacity(&caches, &key, load));
    let first = first.expect("first refresh");
    assert_eq!(first, second.expect("coalesced refresh"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    caches.current.invalidate(&key).await;
    let unchanged = cached_or_load_capacity(&caches, &key, load)
        .await
        .expect("refresh unchanged content");
    assert_eq!(first, unchanged);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let replacement = CapacityCacheKey {
        incarnation: Uuid::new_v4(),
        ..key.clone()
    };
    assert_eq!(cached_or_load_capacity(&caches, &replacement, load).await.unwrap().0, first.0);
    assert_eq!(calls.load(Ordering::SeqCst), 3, "new incarnation must not hit the old cache");
    let different_store = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        ..key.clone()
    };
    assert_eq!(cached_or_load_capacity(&caches, &different_store, load).await.unwrap().0, first.0);
    assert_eq!(calls.load(Ordering::SeqCst), 4, "other stores must not share cached values");

    caches.current.invalidate(&key).await;
    let changed = cached_or_load_capacity(&caches, &key, || async {
        Ok(CapacityFigures {
            capacity: 100,
            used: 30,
            available: 70,
        })
    })
    .await
    .expect("refresh changed content");
    assert_ne!(first.1, changed.1);
}

#[tokio::test]
async fn different_buckets_refresh_concurrently_even_if_old_stripes_collide() {
    use std::hash::{Hash, Hasher};
    let old_stripe = |bucket: &str| {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        bucket.hash(&mut hash);
        hash.finish() % 64
    };
    let first_bucket = "bucket-a";
    let second_bucket = (0..10_000)
        .map(|n| format!("bucket-{n}"))
        .find(|candidate| old_stripe(candidate) == old_stripe(first_bucket))
        .expect("colliding old stripe");
    let caches = CapacityCaches::new();
    let store_id = Uuid::new_v4();
    let first = CapacityCacheKey {
        store_id,
        bucket: first_bucket.into(),
        incarnation: Uuid::new_v4(),
    };
    let second = CapacityCacheKey {
        store_id,
        bucket: second_bucket,
        incarnation: Uuid::new_v4(),
    };
    let first_started = tokio::sync::Notify::new();
    let second_started = tokio::sync::Notify::new();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            cached_or_load_capacity(&caches, &first, || async {
                first_started.notify_one();
                second_started.notified().await;
                Ok(CapacityFigures {
                    capacity: 1,
                    used: 0,
                    available: 1,
                })
            }),
            cached_or_load_capacity(&caches, &second, || async {
                first_started.notified().await;
                second_started.notify_one();
                Ok(CapacityFigures {
                    capacity: 1,
                    used: 0,
                    available: 1,
                })
            })
        )
    })
    .await
    .expect("independent bucket refreshes must not block each other");
    assert!(result.0.is_ok() && result.1.is_ok());
}

#[tokio::test]
async fn capacity_refresh_admission_is_bounded_and_released_on_cancel() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut caches = CapacityCaches::new();
    caches.refresh_permits = tokio::sync::Semaphore::new(1);
    let caches = Arc::new(caches);
    let first = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        bucket: "first".into(),
        incarnation: Uuid::new_v4(),
    };
    let second = CapacityCacheKey {
        bucket: "second".into(),
        incarnation: Uuid::new_v4(),
        ..first.clone()
    };
    let started = Arc::new(tokio::sync::Notify::new());
    let first_caches = Arc::clone(&caches);
    let first_started = Arc::clone(&started);
    let first_task = tokio::spawn(async move {
        cached_or_load_capacity(&first_caches, &first, || async {
            first_started.notify_one();
            std::future::pending::<()>().await;
            Ok(CapacityFigures {
                capacity: 1,
                used: 0,
                available: 1,
            })
        })
        .await
    });
    started.notified().await;
    let second_caches = Arc::clone(&caches);
    let calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::clone(&calls);
    let (called_tx, called_rx) = tokio::sync::oneshot::channel();
    let second_task = tokio::spawn(async move {
        let _ = called_tx.send(());
        cached_or_load_capacity(&second_caches, &second, || async {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CapacityFigures {
                capacity: 1,
                used: 0,
                available: 1,
            })
        })
        .await
    });
    called_rx.await.expect("second request started");
    tokio::task::yield_now().await;
    assert_eq!(caches.refresh_permits.available_permits(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    first_task.abort();
    assert!(first_task.await.expect_err("first refresh cancelled").is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), second_task)
        .await
        .expect("permit released")
        .expect("second task joined")
        .expect("second refresh succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(caches.refresh_permits.available_permits(), 1);
}

#[tokio::test]
async fn concurrent_capacity_failures_are_coalesced_and_retryable() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let caches = CapacityCaches::new();
    let key = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        bucket: "bucket".into(),
        incarnation: Uuid::new_v4(),
    };
    let calls = AtomicUsize::new(0);
    let fail = || async {
        calls.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Err::<CapacityFigures, _>(capacity_unavailable())
    };
    let (first, second) =
        tokio::join!(cached_or_load_capacity(&caches, &key, fail), cached_or_load_capacity(&caches, &key, fail));
    assert_eq!(first.unwrap_err().code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(second.unwrap_err().code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let recovered = cached_or_load_capacity(&caches, &key, || async {
        Ok(CapacityFigures {
            capacity: 1,
            used: 0,
            available: 1,
        })
    })
    .await
    .expect("failed refresh must remain retryable");
    assert!(!recovered.0.is_empty());
}

#[test]
fn cluster_capacity_rejects_missing_or_stale_observations() {
    let mut info = rustfs_madmin::StorageInfo {
        disks: vec![rustfs_madmin::Disk {
            state: "ok".into(),
            capacity_observation_source: Some("live_probe".into()),
            ..Default::default()
        }],
        observations: vec![rustfs_madmin::StorageInfoObservation {
            status: rustfs_madmin::StorageInfoProbeStatus::Succeeded,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(cluster_capacity_observations_are_live(&info));
    info.disks[0].capacity_observation_source = Some("snapshot".into());
    assert!(!cluster_capacity_observations_are_live(&info));
    info.disks[0].capacity_observation_source = Some("live_probe".into());
    info.observations[0].status = rustfs_madmin::StorageInfoProbeStatus::Failed;
    assert!(!cluster_capacity_observations_are_live(&info));
    info.observations.clear();
    assert!(!cluster_capacity_observations_are_live(&info));
}

#[test]
fn cluster_capacity_uses_the_smallest_disk_in_each_erasure_set() {
    let mut info = rustfs_madmin::StorageInfo::default();
    info.backend.drives_per_set = vec![4];
    info.backend.total_sets = vec![1];
    info.backend.standard_sc_data = vec![2];
    info.backend.standard_sc_parities = vec![2];
    info.disks = [100, 100, 100, 10]
        .into_iter()
        .enumerate()
        .map(|(index, free)| rustfs_madmin::Disk {
            pool_index: 0,
            set_index: 0,
            disk_index: i32::try_from(index).unwrap(),
            total_space: 200,
            available_space: free,
            ..Default::default()
        })
        .collect();
    assert_eq!(conservative_cluster_capacity(&info), Some((400, 20)));
    info.disks[3].disk_index = 0;
    assert_eq!(conservative_cluster_capacity(&info), None, "duplicate drives cannot certify capacity");
    info.disks.pop();
    assert_eq!(conservative_cluster_capacity(&info), None, "missing drives cannot certify capacity");
}

#[test]
fn cluster_capacity_handles_heterogeneous_pools_and_rejects_overflow() {
    let mut info = rustfs_madmin::StorageInfo::default();
    info.backend.drives_per_set = vec![2, 1];
    info.backend.total_sets = vec![1, 2];
    info.backend.standard_sc_data = vec![1, 1];
    info.backend.standard_sc_parities = vec![1, 0];
    info.disks = [
        (0, 0, 0, 100, 80),
        (0, 0, 1, 120, 50),
        (1, 0, 0, 200, 90),
        (1, 1, 0, 300, 150),
    ]
    .into_iter()
    .map(|(pool, set, index, total, free)| rustfs_madmin::Disk {
        pool_index: pool,
        set_index: set,
        disk_index: index,
        total_space: total,
        available_space: free,
        ..Default::default()
    })
    .collect();
    assert_eq!(conservative_cluster_capacity(&info), Some((600, 290)));
    info.disks[2].total_space = u64::MAX;
    info.disks[3].total_space = u64::MAX;
    assert_eq!(conservative_cluster_capacity(&info), None);
}

#[tokio::test]
async fn cluster_capacity_refresh_is_shared_and_store_scoped() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = ClusterCapacityCache::new();
    let calls = AtomicUsize::new(0);
    let store = Uuid::new_v4();
    let load = || async {
        calls.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok((100, 80))
    };
    let (a, b) = tokio::join!(
        cached_cluster_capacity_with(&cache, store, load),
        cached_cluster_capacity_with(&cache, store, load)
    );
    assert_eq!((a.unwrap(), b.unwrap()), ((100, 80), (100, 80)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(cached_cluster_capacity_with(&cache, Uuid::new_v4(), load).await.unwrap(), (100, 80));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let failed_store = Uuid::new_v4();
    let failures = AtomicUsize::new(0);
    let fail = || async {
        failures.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Err::<(u64, u64), _>(capacity_unavailable())
    };
    let (a, b) = tokio::join!(
        cached_cluster_capacity_with(&cache, failed_store, fail),
        cached_cluster_capacity_with(&cache, failed_store, fail)
    );
    assert_eq!(a.unwrap_err().code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(b.unwrap_err().code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(failures.load(Ordering::SeqCst), 1, "concurrent failed probes are coalesced");
    assert_eq!(cached_cluster_capacity_with(&cache, failed_store, load).await.unwrap(), (100, 80));
    assert_eq!(calls.load(Ordering::SeqCst), 3, "failed probes must not poison the cache");
}

#[tokio::test]
async fn cancelled_cluster_probe_releases_bounded_admission() {
    let mut cache = ClusterCapacityCache::new();
    cache.refresh_permits = tokio::sync::Semaphore::new(1);
    let cache = Arc::new(cache);
    let first_cache = Arc::clone(&cache);
    let started = Arc::new(tokio::sync::Notify::new());
    let first_started = Arc::clone(&started);
    let first = tokio::spawn(async move {
        cached_cluster_capacity_with(&first_cache, Uuid::new_v4(), || async {
            first_started.notify_one();
            std::future::pending::<()>().await;
            Ok((1, 1))
        })
        .await
    });
    started.notified().await;
    assert_eq!(cache.refresh_permits.available_permits(), 0);
    first.abort();
    assert!(first.await.expect_err("probe was cancelled").is_cancelled());
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        cached_cluster_capacity_with(&cache, Uuid::new_v4(), || async { Ok((2, 1)) }),
    )
    .await
    .expect("cancelled probe released admission")
    .expect("next probe succeeds");
    assert_eq!(result, (2, 1));
    assert_eq!(cache.refresh_permits.available_permits(), 1);
}

#[tokio::test]
async fn cancelled_capacity_refresh_can_be_retried() {
    let caches = Arc::new(CapacityCaches::new());
    let key = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        bucket: "bucket".into(),
        incarnation: Uuid::new_v4(),
    };
    let started = Arc::new(tokio::sync::Notify::new());
    let task_key = key.clone();
    let task_caches = Arc::clone(&caches);
    let task_started = Arc::clone(&started);
    let task = tokio::spawn(async move {
        cached_or_load_capacity(&task_caches, &task_key, || async {
            task_started.notify_one();
            std::future::pending::<()>().await;
            Ok(CapacityFigures {
                capacity: 1,
                used: 0,
                available: 1,
            })
        })
        .await
    });
    started.notified().await;
    task.abort();
    assert!(task.await.expect_err("refresh was cancelled").is_cancelled());
    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        cached_or_load_capacity(&caches, &key, || async {
            Ok(CapacityFigures {
                capacity: 1,
                used: 0,
                available: 1,
            })
        }),
    )
    .await
    .expect("cancelled refresh released the waiters")
    .expect("retry succeeds");
    assert!(!recovered.0.is_empty());
}

#[tokio::test]
async fn cancelled_capacity_leader_hands_over_to_waiter() {
    let caches = Arc::new(CapacityCaches::new());
    let key = CapacityCacheKey {
        store_id: Uuid::new_v4(),
        bucket: "bucket".into(),
        incarnation: Uuid::new_v4(),
    };
    let started = Arc::new(tokio::sync::Notify::new());
    let leader_caches = Arc::clone(&caches);
    let leader_key = key.clone();
    let leader_started = Arc::clone(&started);
    let leader = tokio::spawn(async move {
        cached_or_load_capacity(&leader_caches, &leader_key, || async {
            leader_started.notify_one();
            std::future::pending::<()>().await;
            Ok(CapacityFigures {
                capacity: 1,
                used: 0,
                available: 1,
            })
        })
        .await
    });
    started.notified().await;
    let mut waiter = Box::pin(cached_or_load_capacity(&caches, &key, || async {
        Ok(CapacityFigures {
            capacity: 2,
            used: 0,
            available: 2,
        })
    }));
    tokio::select! {
        biased;
        result = &mut waiter => panic!("waiter finished before leader cancellation: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    leader.abort();
    assert!(leader.await.expect_err("leader cancelled").is_cancelled());
    let recovered = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter resumes")
        .expect("waiter refresh succeeds");
    assert!(!recovered.0.is_empty());
}

#[test]
fn etag_is_md5_of_the_full_body_and_range_slices_it() {
    let xml = capacity_xml(&CapacityFigures {
        capacity: 10,
        used: 4,
        available: 3,
    });
    let bytes = Bytes::from(xml.clone());
    let etag = md5_hex(&bytes);
    assert_eq!(etag.len(), 32);
    let full = apply_range(bytes.clone(), etag.clone(), OffsetDateTime::UNIX_EPOCH, None).expect("full read");
    assert_eq!(full.body.as_ref(), xml.as_bytes());
    assert!(full.content_range.is_none());
    assert_eq!(full.etag_hex, etag);
    assert_eq!(full.content_length, i64::try_from(xml.len()).unwrap_or(i64::MAX));

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 0,
        end: 3,
    };
    let sliced = apply_range(bytes.clone(), etag.clone(), OffsetDateTime::UNIX_EPOCH, Some(&range)).expect("range");
    assert_eq!(sliced.body.as_ref(), &xml.as_bytes()[..4]);
    assert_eq!(sliced.content_length, 4);
    assert_eq!(sliced.content_range.as_deref(), Some(format!("bytes 0-3/{}", xml.len()).as_str()));
    assert_eq!(sliced.etag_hex, etag);

    let past_end = HTTPRangeSpec {
        is_suffix_length: false,
        start: i64::try_from(xml.len()).unwrap_or(i64::MAX),
        end: -1,
    };
    assert_eq!(
        apply_range(bytes, etag, OffsetDateTime::UNIX_EPOCH, Some(&past_end)).err(),
        Some(SosapiReadError::InvalidRange)
    );
}

#[test]
#[serial_test::serial]
fn veeam_sosapi_http_contract() {
    crate::app::gating_test_env::run_large_stack_test("veeam-sosapi-http", || async {
        temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, None::<&str>)], async {
            use crate::app::runtime_sources::{AppContext, ServerContextSlot};
            use crate::app::storage_api::s3::{Body, S3ServiceBuilder, SimpleAuth};
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
            use crate::app::storage_api::test::contract::list::ListOperations as _;
            use crate::app::storage_api::test::contract::object::ObjectOperations as _;
            use http::Method;
            use http_body_util::BodyExt;

            let (paths, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
            let _paths = paths;
            let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
            let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
            let slot = ServerContextSlot::new();
            assert!(slot.install(context));
            let fs = super::super::FS::with_server_ctx(slot);
            let owner = S3ServiceBuilder::new(fs.clone()).build();
            let mut anonymous_builder = S3ServiceBuilder::new(fs.clone());
            anonymous_builder.set_auth(SimpleAuth::from_single("test-access", "test-secret"));
            anonymous_builder.set_access(fs.clone());
            let anonymous = anonymous_builder.build();
            let mut bad_auth_builder = S3ServiceBuilder::new(fs.clone());
            bad_auth_builder.set_auth(SimpleAuth::from_single("test-access", "test-secret"));
            bad_auth_builder.set_access(fs);
            let bad_auth = bad_auth_builder.build();

            let bucket = format!("veeam-sosapi-{}", super::super::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create sosapi bucket");

            let owner_call = |method: Method, uri: String, headers: Vec<(&str, String)>, body: Bytes| {
                let mut builder = http::Request::builder().method(method).uri(uri).header("host", "localhost");
                for (name, value) in headers {
                    builder = builder.header(name, value);
                }
                builder
                    .extension(crate::storage::access::ReqInfo {
                        cred: Some(rustfs_credentials::Credentials::default()),
                        is_owner: true,
                        ..Default::default()
                    })
                    .body(Body::from(body))
                    .expect("HTTP request")
            };

            let system_uri = format!("/{bucket}/{SYSTEM_KEY}");
            let capacity_uri = format!("/{bucket}/{CAPACITY_KEY}");

            let response = owner
                .call(owner_call(Method::GET, system_uri.clone(), Vec::new(), Bytes::new()))
                .await
                .expect("GET system.xml");
            assert_eq!(response.status(), http::StatusCode::OK);
            assert_eq!(response.headers()["content-type"], "application/xml");
            assert!(response.headers().get("etag").is_some());
            assert!(response.headers().get("last-modified").is_some());
            let system_etag = response.headers()["etag"].to_str().expect("etag").to_string();
            let system_modified = response.headers()["last-modified"].to_str().expect("last modified").to_string();
            let system_body = BodyExt::collect(response.into_body())
                .await
                .expect("system body")
                .to_bytes();
            let system_xml = std::str::from_utf8(&system_body).expect("utf-8");
            assert!(system_xml.contains("<ProtocolVersion>\"1.0\"</ProtocolVersion>"));
            assert!(system_xml.contains(&format!("<ModelName>\"RustFS {}\"</ModelName>", env!("CARGO_PKG_VERSION"))));
            assert!(!system_xml.contains("SystemRecommendations"));
            assert!(!system_xml.contains("APIEndpoints"));

            let head = owner
                .call(owner_call(Method::HEAD, system_uri.clone(), Vec::new(), Bytes::new()))
                .await
                .expect("HEAD system.xml");
            assert_eq!(head.status(), http::StatusCode::OK);
            assert_eq!(
                head.headers()["content-length"],
                system_body.len().to_string().as_str()
            );
            assert_eq!(head.headers()["etag"].to_str().expect("head etag"), system_etag.as_str());
            assert_eq!(head.headers()["content-type"], "application/xml");

            for method in [Method::GET, Method::HEAD] {
                let unchanged = owner.call(owner_call(method, system_uri.clone(),
                    vec![("if-modified-since", system_modified.clone())], Bytes::new()))
                    .await.expect("unchanged system XML");
                assert_eq!(unchanged.status(), http::StatusCode::NOT_MODIFIED);
            }

            let ranged = owner
                .call(owner_call(
                    Method::GET,
                    system_uri.clone(),
                    vec![("range", "bytes=0-3".to_string())],
                    Bytes::new(),
                ))
                .await
                .expect("ranged GET");
            assert_eq!(ranged.status(), http::StatusCode::PARTIAL_CONTENT);
            assert_eq!(ranged.headers()["etag"].to_str().expect("range etag"), system_etag.as_str());
            let content_range = ranged.headers()["content-range"].to_str().expect("content-range");
            assert!(content_range.starts_with("bytes 0-3/"), "{content_range}");
            let ranged_body = BodyExt::collect(ranged.into_body()).await.expect("range body").to_bytes();
            assert_eq!(&ranged_body[..], &system_body[..4]);

            for method in [Method::GET, Method::HEAD] {
                let missing_version = owner.call(owner_call(
                    method, format!("{system_uri}?versionId=11111111-1111-4111-8111-111111111111"), Vec::new(), Bytes::new()
                )).await.expect("versioned read");
                assert_eq!(missing_version.status(), http::StatusCode::NOT_FOUND);
            }

            let unknown_capacity = owner.call(owner_call(Method::GET, capacity_uri.clone(), Vec::new(), Bytes::new()))
                .await.expect("unknown capacity");
            assert_eq!(unknown_capacity.status(), http::StatusCode::SERVICE_UNAVAILABLE);
            let mut snapshot = rustfs_data_usage::DataUsageInfo {
                last_update: Some(std::time::SystemTime::now() + Duration::from_secs(1)),
                buckets_count: 1,
                usage_snapshot_complete: true,
                ..Default::default()
            };
            let bucket_incarnation = store.bucket_incarnation_id_from_disk(&bucket).await.expect("bucket incarnation");
            snapshot.usage_snapshot_converged = Some(true);
            snapshot.scanner_cycle = Some(1);
            snapshot.scanner_epoch = Some(1);
            snapshot.buckets_usage.insert(bucket.clone(), rustfs_data_usage::BucketUsageInfo {
                bucket_incarnation: Some(bucket_incarnation.to_string()), ..Default::default()
            });
            snapshot.bucket_sizes.insert(bucket.clone(), 0);
            crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                .await.expect("seed capacity snapshot");

            let capacity = owner
                .call(owner_call(Method::GET, capacity_uri.clone(), Vec::new(), Bytes::new()))
                .await
                .expect("GET capacity.xml");
            assert_eq!(capacity.status(), http::StatusCode::OK);
            assert_eq!(capacity.headers()["content-type"], "application/xml");
            let capacity_etag = capacity.headers()["etag"].to_str().expect("capacity etag").to_string();
            let capacity_body = BodyExt::collect(capacity.into_body())
                .await
                .expect("capacity body")
                .to_bytes();
            let capacity_xml = std::str::from_utf8(&capacity_body).expect("utf-8");
            let capacity_value: u64 = element_text(capacity_xml, "Capacity").parse().expect("capacity");
            let available_value: u64 = element_text(capacity_xml, "Available").parse().expect("available");
            let used_value: u64 = element_text(capacity_xml, "Used").parse().expect("used");
            assert!(used_value <= capacity_value);
            assert!(available_value <= capacity_value.saturating_sub(used_value));

            let other_etag = "\"00000000000000000000000000000000\"";
            let past = OffsetDateTime::UNIX_EPOCH
                .format(&super::super::RFC1123)
                .expect("past http date");
            let future = (OffsetDateTime::UNIX_EPOCH + time::Duration::days(40_000))
                .format(&super::super::RFC1123)
                .expect("future http date");
            let precondition_cases = [
                (Method::GET, system_uri.as_str(), "if-match", system_etag.as_str(), http::StatusCode::OK),
                (Method::HEAD, system_uri.as_str(), "if-match", system_etag.as_str(), http::StatusCode::OK),
                (Method::GET, system_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                (Method::HEAD, system_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                (Method::GET, system_uri.as_str(), "if-none-match", system_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::HEAD, system_uri.as_str(), "if-none-match", system_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::GET, system_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                (Method::HEAD, system_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                (Method::GET, capacity_uri.as_str(), "if-match", capacity_etag.as_str(), http::StatusCode::OK),
                (Method::HEAD, capacity_uri.as_str(), "if-match", capacity_etag.as_str(), http::StatusCode::OK),
                (Method::GET, capacity_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                (Method::HEAD, capacity_uri.as_str(), "if-match", other_etag, http::StatusCode::PRECONDITION_FAILED),
                (Method::GET, capacity_uri.as_str(), "if-none-match", capacity_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::HEAD, capacity_uri.as_str(), "if-none-match", capacity_etag.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::GET, capacity_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                (Method::HEAD, capacity_uri.as_str(), "if-none-match", other_etag, http::StatusCode::OK),
                (Method::GET, system_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                (Method::HEAD, system_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                (Method::GET, system_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::HEAD, system_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::GET, system_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                (Method::HEAD, system_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                (Method::GET, system_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                (Method::HEAD, system_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                (Method::GET, capacity_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                (Method::HEAD, capacity_uri.as_str(), "if-modified-since", past.as_str(), http::StatusCode::OK),
                (Method::GET, capacity_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::HEAD, capacity_uri.as_str(), "if-modified-since", future.as_str(), http::StatusCode::NOT_MODIFIED),
                (Method::GET, capacity_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                (Method::HEAD, capacity_uri.as_str(), "if-unmodified-since", future.as_str(), http::StatusCode::OK),
                (Method::GET, capacity_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
                (Method::HEAD, capacity_uri.as_str(), "if-unmodified-since", past.as_str(), http::StatusCode::PRECONDITION_FAILED),
            ];
            for (method, uri, header, value, expected) in precondition_cases {
                let response = owner
                    .call(owner_call(
                        method.clone(),
                        uri.to_string(),
                        vec![(header, value.to_string())],
                        Bytes::new(),
                    ))
                    .await
                    .unwrap_or_else(|err| panic!("{method} {uri} {header} failed: {err}"));
                let status = response.status();
                let response_etag = response
                    .headers()
                    .get(http::header::ETAG)
                    .and_then(|etag| etag.to_str().ok())
                    .map(str::to_string);
                let body = BodyExt::collect(response.into_body()).await.expect("precondition body").to_bytes();
                let body_text = String::from_utf8_lossy(&body);
                assert_eq!(status, expected, "{method} {uri} {header}={value} body={body_text}");
                let expected_etag = if uri.ends_with(SYSTEM_KEY) { &system_etag } else { &capacity_etag };
                match expected {
                    http::StatusCode::OK if method == Method::GET => {
                        assert!(body_text.contains("<?xml"), "{method} {uri} {header} body={body_text}");
                    }
                    http::StatusCode::NOT_MODIFIED => {
                        assert_eq!(response_etag.as_deref(), Some(expected_etag.as_str()));
                        assert!(!body_text.contains("ProtocolVersion"), "{body_text}");
                        assert!(!body_text.contains("<Capacity>"), "{body_text}");
                    }
                    http::StatusCode::PRECONDITION_FAILED if method == Method::GET => {
                        assert!(body_text.contains("PreconditionFailed"), "{body_text}");
                        assert!(!body_text.contains("ProtocolVersion"), "{body_text}");
                    }
                    _ => {}
                }
            }
            let ranged_match = owner
                .call(owner_call(
                    Method::GET,
                    system_uri.clone(),
                    vec![
                        ("if-match", system_etag.clone()),
                        ("range", "bytes=0-3".to_string()),
                    ],
                    Bytes::new(),
                ))
                .await
                .expect("matching If-Match still honors range");
            assert_eq!(ranged_match.status(), http::StatusCode::PARTIAL_CONTENT);
            let part_and_match = owner
                .call(owner_call(
                    Method::GET,
                    format!("{system_uri}?partNumber=1"),
                    vec![("if-match", other_etag.to_string())],
                    Bytes::new(),
                ))
                .await
                .expect("failed If-Match precedes partNumber");
            assert_eq!(part_and_match.status(), http::StatusCode::PRECONDITION_FAILED);

            let metadata = crate::app::storage_api::test::get_global_bucket_metadata_sys().expect("bucket metadata");
            let mut checker = crate::app::storage_api::test::bucket::quota::checker::QuotaChecker::new(Arc::clone(&metadata));
            let quota_bucket = format!("veeam-sosapi-quota-{}", super::super::Uuid::new_v4());
            store
                .make_bucket(&quota_bucket, &MakeBucketOptions::default())
                .await
                .expect("quota bucket");
            checker
                .set_quota_config(
                    &quota_bucket,
                    crate::app::storage_api::object_usecase::bucket::quota::BucketQuota::new(Some(1_000_000)),
                )
                .await
                .expect("set quota on fresh bucket");
            snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(2));
            snapshot.buckets_count = 2;
            let quota_incarnation = store.bucket_incarnation_id_from_disk(&quota_bucket).await.expect("quota bucket incarnation");
            snapshot.buckets_usage.insert(quota_bucket.clone(), rustfs_data_usage::BucketUsageInfo {
                bucket_incarnation: Some(quota_incarnation.to_string()), ..Default::default()
            });
            snapshot.bucket_sizes.insert(quota_bucket.clone(), 0);
            crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                .await.expect("seed quota bucket usage");
            let quota_response = owner
                .call(owner_call(
                    Method::GET,
                    format!("/{quota_bucket}/{CAPACITY_KEY}"),
                    Vec::new(),
                    Bytes::new(),
                ))
                .await
                .expect("quota capacity");
            assert_eq!(quota_response.status(), http::StatusCode::OK);
            let quota_xml = BodyExt::collect(quota_response.into_body())
                .await
                .expect("quota body")
                .to_bytes();
            let quota_xml = std::str::from_utf8(&quota_xml).expect("utf-8");
            assert_eq!(element_text(quota_xml, "Capacity"), "1000000");
            let quota_used: u64 = element_text(quota_xml, "Used").parse().expect("quota used");
            let quota_available: u64 = element_text(quota_xml, "Available").parse().expect("quota available");
            assert!(quota_used <= 1_000_000);
            assert!(quota_available <= 1_000_000 - quota_used);
            let (mut corrupt_quota, _) = metadata.read().await.get_config(&quota_bucket).await.expect("load quota metadata");
            let original_quota = Arc::clone(&corrupt_quota);
            Arc::make_mut(&mut corrupt_quota).quota_config_json = b"{invalid-json".to_vec();
            metadata.read().await.set(quota_bucket.clone(), corrupt_quota).await;
            capacity_caches().current.invalidate(&CapacityCacheKey {
                store_id: store.id, bucket: quota_bucket.clone(), incarnation: quota_incarnation,
            }).await;
            for method in [Method::GET, Method::HEAD] {
                let response = owner.call(owner_call(
                    method, format!("/{quota_bucket}/{CAPACITY_KEY}"), Vec::new(), Bytes::new()
                )).await.expect("invalid quota response");
                assert_eq!(response.status(), http::StatusCode::OK, "stale in-memory quota must not override authoritative metadata");
            }
            metadata.read().await.set(quota_bucket.clone(), original_quota).await;

            let recycled = format!("veeam-recycled-{}", Uuid::new_v4());
            store.make_bucket(&recycled, &MakeBucketOptions::default()).await.expect("create recycled bucket");
            let old_id = store.bucket_incarnation_id_from_disk(&recycled).await.expect("old identity");
            let (old_metadata, _) = metadata.read().await.get_config(&recycled).await.expect("old cached metadata");
            snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(3));
            snapshot.buckets_count = 3;
            snapshot.buckets_usage.insert(recycled.clone(), rustfs_data_usage::BucketUsageInfo {
                size: 123, bucket_incarnation: Some(old_id.to_string()), ..Default::default()
            });
            snapshot.bucket_sizes.insert(recycled.clone(), 123);
            crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                .await.expect("seed old incarnation snapshot");
            let recycled_uri = format!("/{recycled}/{CAPACITY_KEY}");
            let old = owner.call(owner_call(Method::GET, recycled_uri.clone(), Vec::new(), Bytes::new())).await.expect("old capacity");
            assert_eq!(old.status(), http::StatusCode::OK);
            let old_body = BodyExt::collect(old.into_body()).await.expect("old capacity body").to_bytes();
            assert_eq!(element_text(std::str::from_utf8(&old_body).unwrap(), "Used"), "123");
            store.delete_bucket(&recycled, &DeleteBucketOptions::default()).await.expect("delete recycled bucket");
            store.make_bucket(&recycled, &MakeBucketOptions::default()).await.expect("recreate bucket");
            let new_id = store.bucket_incarnation_id_from_disk(&recycled).await.expect("new identity");
            assert_ne!(old_id, new_id);
            let (new_metadata, _) = metadata.read().await.get_config(&recycled).await.expect("new cached metadata");
            metadata.read().await.set(recycled.clone(), old_metadata).await;
            // Simulate a stale peer snapshot after the new bucket exists.
            snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(4));
            crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot.clone(), Arc::clone(&store))
                .await.expect("seed stale incarnation snapshot");
            let stale = owner.call(owner_call(Method::GET, recycled_uri.clone(), Vec::new(), Bytes::new())).await.expect("stale capacity");
            assert_eq!(stale.status(), http::StatusCode::SERVICE_UNAVAILABLE);
            snapshot.last_update = Some(std::time::SystemTime::now() + Duration::from_secs(5));
            snapshot.buckets_usage.get_mut(&recycled).unwrap().bucket_incarnation = Some(new_id.to_string());
            snapshot.buckets_usage.get_mut(&recycled).unwrap().size = 17;
            snapshot.bucket_sizes.insert(recycled.clone(), 17);
            crate::app::storage_api::test::data_usage::store_data_usage_in_backend(snapshot, Arc::clone(&store))
                .await.expect("seed new incarnation snapshot");
            let fresh = owner.call(owner_call(Method::GET, recycled_uri, Vec::new(), Bytes::new())).await.expect("new capacity");
            assert_eq!(fresh.status(), http::StatusCode::OK);
            let fresh_body = BodyExt::collect(fresh.into_body()).await.expect("new capacity body").to_bytes();
            assert_eq!(element_text(std::str::from_utf8(&fresh_body).unwrap(), "Used"), "17");
            metadata.read().await.set(recycled.clone(), new_metadata).await;

            let stored = store
                .get_object_info(&bucket, SYSTEM_KEY, &super::super::ObjectOptions::default())
                .await;
            assert!(stored.is_err(), "virtual GET must not create xl.meta");

            let mutations = [
                (Method::PUT, system_uri.clone(), Vec::new(), Bytes::from_static(b"nope")),
                (Method::DELETE, system_uri.clone(), Vec::new(), Bytes::new()),
                (
                    Method::PUT,
                    capacity_uri.clone(),
                    vec![("x-amz-copy-source", format!("/{bucket}/other"))],
                    Bytes::new(),
                ),
                (Method::POST, format!("{system_uri}?uploads"), Vec::new(), Bytes::new()),
                (
                    Method::PUT,
                    format!("{system_uri}?partNumber=1&uploadId=abc"),
                    Vec::new(),
                    Bytes::from_static(b"part"),
                ),
                (
                    Method::POST,
                    format!("{system_uri}?uploadId=abc"),
                    Vec::new(),
                    Bytes::from_static(
                        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part></CompleteMultipartUpload>",
                    ),
                ),
                (Method::DELETE, format!("{system_uri}?uploadId=abc"), Vec::new(), Bytes::new()),
                (Method::GET, format!("{system_uri}?uploadId=abc"), Vec::new(), Bytes::new()),
                (Method::GET, format!("{system_uri}?tagging"), Vec::new(), Bytes::new()),
                (
                    Method::PUT,
                    format!("{system_uri}?tagging"),
                    Vec::new(),
                    Bytes::from_static(
                        b"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>",
                    ),
                ),
                (Method::DELETE, format!("{system_uri}?tagging"), Vec::new(), Bytes::new()),
                (Method::GET, format!("{system_uri}?retention"), Vec::new(), Bytes::new()),
                (
                    Method::PUT,
                    format!("{system_uri}?retention"),
                    Vec::new(),
                    Bytes::from_static(
                        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00.000Z</RetainUntilDate></Retention>",
                    ),
                ),
                (Method::GET, format!("{system_uri}?legal-hold"), Vec::new(), Bytes::new()),
                (
                    Method::PUT,
                    format!("{system_uri}?legal-hold"),
                    Vec::new(),
                    Bytes::from_static(b"<LegalHold><Status>ON</Status></LegalHold>"),
                ),
                (
                    Method::POST,
                    format!("{system_uri}?restore"),
                    Vec::new(),
                    Bytes::from_static(b"<RestoreRequest><Days>1</Days></RestoreRequest>"),
                ),
            ];
            for (method, uri, headers, body) in mutations {
                let response = owner
                    .call(owner_call(method.clone(), uri.clone(), headers, body))
                    .await
                    .unwrap_or_else(|err| panic!("{method} {uri} failed: {err}"));
                let status = response.status();
                let error_body = BodyExt::collect(response.into_body())
                    .await
                    .expect("mutation body")
                    .to_bytes();
                let error_xml = String::from_utf8_lossy(&error_body);
                assert_eq!(
                    status,
                    http::StatusCode::METHOD_NOT_ALLOWED,
                    "{method} {uri} body={error_xml}"
                );
                assert!(error_xml.contains("MethodNotAllowed"), "{method} {uri} body={error_xml}");
            }
            assert!(
                store
                    .get_object_info(&bucket, SYSTEM_KEY, &super::super::ObjectOptions::default())
                    .await
                    .is_err()
            );

            for key in [SYSTEM_KEY, CAPACITY_KEY] {
                let archive = empty_member_archive(key);
                let result = owner.call(owner_call(
                    Method::PUT,
                    format!("/{bucket}/archive.tar"),
                    vec![("x-amz-meta-snowball-auto-extract", "true".to_string()),
                         ("content-length", archive.len().to_string())],
                    archive,
                )).await.expect("archive extract response");
                assert_eq!(result.status(), http::StatusCode::METHOD_NOT_ALLOWED, "archive must not create {key}");
                assert!(store.get_object_info(&bucket, key, &super::super::ObjectOptions::default()).await.is_err(),
                    "archive member must not create stored metadata");
            }

            let archive_bucket = format!("veeam-archive-disabled-{}", Uuid::new_v4());
            store.make_bucket(&archive_bucket, &MakeBucketOptions::default()).await.expect("archive test bucket");
            temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"))], async {
                let archive = empty_member_archive(SYSTEM_KEY);
                let result = owner.call(owner_call(
                    Method::PUT,
                    format!("/{archive_bucket}/archive.tar"),
                    vec![("x-amz-meta-snowball-auto-extract", "true".to_string()),
                         ("content-length", archive.len().to_string())],
                    archive,
                )).await.expect("disabled archive response");
                assert_eq!(result.status(), http::StatusCode::OK, "disabled feature restores archive extraction");
                assert!(store.get_object_info(&archive_bucket, SYSTEM_KEY, &super::super::ObjectOptions::default()).await.is_ok());
            }).await;

            let normal_key = "Veeam/Backup/job.dat";
            let normal = owner
                .call(owner_call(
                    Method::PUT,
                    format!("/{bucket}/{normal_key}"),
                    Vec::new(),
                    Bytes::from_static(b"backup-bytes"),
                ))
                .await
                .expect("normal put");
            assert_eq!(normal.status(), http::StatusCode::OK);
            let normal_get = owner
                .call(owner_call(
                    Method::GET,
                    format!("/{bucket}/{normal_key}"),
                    Vec::new(),
                    Bytes::new(),
                ))
                .await
                .expect("normal get");
            assert_eq!(normal_get.status(), http::StatusCode::OK);
            let normal_body = BodyExt::collect(normal_get.into_body()).await.expect("normal body").to_bytes();
            assert_eq!(&normal_body[..], b"backup-bytes");

            let locked = format!("veeam-sosapi-lock-{}", super::super::Uuid::new_v4());
            store
                .make_bucket(
                    &locked,
                    &MakeBucketOptions {
                        lock_enabled: true,
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("object lock bucket");
            let locked_get = owner
                .call(owner_call(
                    Method::GET,
                    format!("/{locked}/{SYSTEM_KEY}"),
                    Vec::new(),
                    Bytes::new(),
                ))
                .await
                .expect("locked get");
            assert_eq!(locked_get.status(), http::StatusCode::OK);
            let locked_put = owner
                .call(owner_call(
                    Method::PUT,
                    format!("/{locked}/{SYSTEM_KEY}"),
                    Vec::new(),
                    Bytes::from_static(b"locked"),
                ))
                .await
                .expect("locked put");
            assert_eq!(locked_put.status(), http::StatusCode::METHOD_NOT_ALLOWED);
            let versions = Arc::clone(&store)
                .list_object_versions(&locked, SYSTEM_KEY, None, None, None, 1000)
                .await
                .expect("list versions");
            assert!(versions.objects.is_empty());

            let anon = anonymous
                .call(
                    http::Request::builder()
                        .method(Method::GET)
                        .uri(system_uri.clone())
                        .header("host", "localhost")
                        .body(Body::empty())
                        .expect("anon request"),
                )
                .await
                .expect("anonymous GET");
            let anon_status = anon.status();
            let anon_body = BodyExt::collect(anon.into_body()).await.expect("anon body").to_bytes();
            let anon_xml = String::from_utf8_lossy(&anon_body);
            assert_eq!(anon_status, http::StatusCode::FORBIDDEN, "{anon_xml}");
            assert!(!anon_xml.contains("ProtocolVersion"), "{anon_xml}");

            let now = OffsetDateTime::now_utc();
            let scope_date = format!("{:04}{:02}{:02}", now.year(), u8::from(now.month()), now.day());
            let amz_date = format!("{scope_date}T{:02}{:02}{:02}Z", now.hour(), now.minute(), now.second());
            let bad = bad_auth
                .call(
                    http::Request::builder()
                        .method(Method::GET)
                        .uri(system_uri.clone())
                        .header("host", "localhost")
                        .header("x-amz-date", &amz_date)
                        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                        .header(
                            "authorization",
                            format!(
                                "AWS4-HMAC-SHA256 Credential=bad/{scope_date}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"
                            ),
                        )
                        .body(Body::empty())
                        .expect("bad auth request"),
                )
                .await
                .expect("bad auth GET");
            let bad_status = bad.status();
            let bad_body = BodyExt::collect(bad.into_body()).await.expect("bad body").to_bytes();
            let bad_xml = String::from_utf8_lossy(&bad_body);
            assert_eq!(bad_status, http::StatusCode::FORBIDDEN, "{bad_xml}");
            assert!(!bad_xml.contains("ProtocolVersion"), "{bad_xml}");

            let missing = owner
                .call(owner_call(
                    Method::PUT,
                    format!("/missing-sosapi-bucket/{SYSTEM_KEY}"),
                    Vec::new(),
                    Bytes::from_static(b"x"),
                ))
                .await
                .expect("missing bucket");
            assert_eq!(missing.status(), http::StatusCode::NOT_FOUND);

            let shadow = format!("veeam-sosapi-shadow-{}", super::super::Uuid::new_v4());
            store
                .make_bucket(&shadow, &MakeBucketOptions::default())
                .await
                .expect("shadow bucket");
            let shadow_uri = format!("/{shadow}/{SYSTEM_KEY}");
            temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"))], async {
                let stored = owner
                    .call(owner_call(
                        Method::PUT,
                        shadow_uri.clone(),
                        Vec::new(),
                        Bytes::from_static(b"shadow-bytes"),
                    ))
                    .await
                    .expect("store shadow");
                assert_eq!(stored.status(), http::StatusCode::OK, "kill switch must allow PUT");
                let missing_virtual = owner
                    .call(owner_call(
                        Method::GET,
                        format!("/{bucket}/{CAPACITY_KEY}"),
                        Vec::new(),
                        Bytes::new(),
                    ))
                    .await
                    .expect("disabled capacity");
                assert_eq!(missing_virtual.status(), http::StatusCode::NOT_FOUND);
            })
            .await;

            for method in [Method::GET, Method::HEAD] {
                let response = owner.call(owner_call(
                    method.clone(), format!("{shadow_uri}?versionId=null"), Vec::new(), Bytes::new()
                )).await.expect("explicit stored version");
                assert_eq!(response.status(), http::StatusCode::OK);
                if method == Method::GET {
                    let body = BodyExt::collect(response.into_body()).await.expect("stored version body").to_bytes();
                    assert_eq!(body, Bytes::from_static(b"shadow-bytes"));
                }
            }

            let virtual_wins = owner
                .call(owner_call(Method::GET, shadow_uri.clone(), Vec::new(), Bytes::new()))
                .await
                .expect("virtual wins");
            assert_eq!(virtual_wins.status(), http::StatusCode::OK);
            let virtual_body = BodyExt::collect(virtual_wins.into_body())
                .await
                .expect("virtual body")
                .to_bytes();
            assert!(std::str::from_utf8(&virtual_body).unwrap_or("").contains("ProtocolVersion"));
            assert!(!virtual_body.windows(12).any(|window| window == b"shadow-bytes"));
            let blocked_delete = owner
                .call(owner_call(Method::DELETE, shadow_uri.clone(), Vec::new(), Bytes::new()))
                .await
                .expect("blocked delete");
            assert_eq!(blocked_delete.status(), http::StatusCode::METHOD_NOT_ALLOWED);
            assert!(
                store
                    .get_object_info(&shadow, SYSTEM_KEY, &super::super::ObjectOptions::default())
                    .await
                    .is_ok(),
                "enabled DELETE must leave the pre-existing object in place"
            );

            temp_env::async_with_vars([(rustfs_config::ENV_VEEAM_SOSAPI_ENABLE, Some("false"))], async {
                let removed = owner
                    .call(owner_call(Method::DELETE, shadow_uri.clone(), Vec::new(), Bytes::new()))
                    .await
                    .expect("delete shadow");
                assert_eq!(removed.status(), http::StatusCode::NO_CONTENT);
            })
            .await;
            assert!(
                store
                    .get_object_info(&shadow, SYSTEM_KEY, &super::super::ObjectOptions::default())
                    .await
                    .is_err()
            );
        })
        .await;
    });
}
