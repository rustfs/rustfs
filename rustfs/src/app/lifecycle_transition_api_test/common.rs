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

//! Shared fixtures for the lifecycle integration tests; owned by no operation batch.

use super::*;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();

pub(super) const TRANSITION_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) const RUSTFS_EXPECTED_CURRENT_VERSION_ID: &str = "x-rustfs-expected-current-version-id";

fn init_tracing() {
    rustfs_test_utils::init_tracing();
}

pub(super) async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_tracing();

    if let Some((paths, ecstore)) = GLOBAL_ENV.get() {
        return (paths.clone(), ecstore.clone());
    }

    // Production startup installs this proof through the notification-system
    // fleet probe. This isolated single-node harness constructs ECStore
    // directly, so model that completed all-v6 probe before any journal permit
    // or background expiry worker can exist.
    install_all_v6_fleet_capability_proof();

    let test_base_dir = format!("/tmp/rustfs_app_lifecycle_test_{}", Uuid::new_v4());
    let temp_dir = PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    let disk_paths = vec![
        temp_dir.join("disk1"),
        temp_dir.join("disk2"),
        temp_dir.join("disk3"),
        temp_dir.join("disk4"),
    ];

    for disk_path in &disk_paths {
        fs::create_dir_all(disk_path).await.unwrap();
    }

    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);
        endpoints.push(endpoint);
    }

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

    super::super::storage_api::test::runtime::init_local_disks(endpoint_pools.clone())
        .await
        .unwrap();

    let server_addr: std::net::SocketAddr = "127.0.0.1:9003".parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    let buckets_list = ecstore
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    lifecycle::bucket_lifecycle_ops::init_background_expiry(ecstore.clone()).await;

    // The usecases resolve the object store through the ambient AppContext
    // since the backlog#1052 InstanceContext migration; without one installed
    // every store-touching usecase call fails with InternalError "Not init".
    // Publish a default-interface context wrapping this test store.
    crate::app::runtime_sources::install_test_app_context(ecstore.clone()).await;

    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

    (disk_paths, ecstore)
}

pub(super) async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(
            bucket_name,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create test bucket");
}

pub(super) async fn upload_test_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) -> ObjectInfo {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    (**ecstore)
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload test object")
}

pub(super) async fn set_bucket_lifecycle_transition_with_tier(
    bucket_name: &str,
    storage_class: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Filter>
            <Prefix>test/</Prefix>
        </Filter>
        <Transition>
          <Days>0</Days>
          <StorageClass>{storage_class}</StorageClass>
        </Transition>
    </Rule>
</LifecycleConfiguration>"#
    );

    metadata_sys::update(bucket_name, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes()).await?;
    Ok(())
}

pub(super) async fn set_bucket_object_lock_enabled(bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let object_lock_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#;
    metadata_sys::update(bucket_name, OBJECT_LOCK_CONFIG, object_lock_xml.as_bytes().to_vec()).await?;
    Ok(())
}

/// Register the shared [`MockWarmBackend`] into this instance's tier config
/// manager. Thin wrapper over the shared `register_mock_tier` helper
/// (rustfs/backlog#1148 ilm-6).
pub(super) async fn register_mock_tier(tier_name: &str) -> MockWarmBackend {
    register_mock_tier_util(&current_tier_config_handle(), tier_name).await
}

pub(super) async fn wait_for_transition(
    ecstore: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    timeout: Duration,
) -> Result<ObjectInfo, String> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let observed = (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await;
        let observed = match observed {
            Ok(info) if info.transitioned_object.status == "complete" => return Ok(info),
            other => other,
        };

        if tokio::time::Instant::now() >= deadline {
            let last_observation = match observed {
                Ok(info) => format!("transitioned_status={:?}", info.transitioned_object.status),
                Err(error) => format!("metadata_error={:?}", error.code()),
            };
            let state = get_global_transition_state();
            return Err(format!(
                "transition did not complete within {timeout:?}; {last_observation}; pending={}, active={}, compensation_pending={}, compensation_running={}",
                state.pending_tasks(),
                state.active_tasks(),
                state.compensation_pending_tasks(),
                state.compensation_running_tasks(),
            ));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// SAFETY: this helper is used only by `#[serial]` tests and runs under the single-threaded Tokio
// runtime (`worker_threads = 1`), so no concurrent test can mutate process environment during the
// `env::set_var` / `env::remove_var` window.
#[allow(unsafe_code)]
pub(super) async fn with_forced_immediate_enqueue_timeout<F, Fut>(backend: &MockWarmBackend, test_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let transition_state = get_global_transition_state();
    let missed_before = transition_state.missed_immediate_tasks();
    let compensation_before = transition_state.compensation_scheduled_tasks();
    let put_barrier = backend.arm_put_barrier().await;
    let original = env::var_os(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT);
    unsafe {
        env::set_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT, "1");
    }
    let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;
    match original {
        Some(value) => unsafe {
            env::set_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT, value);
        },
        None => unsafe {
            env::remove_var(ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT);
        },
    }
    if let Err(err) = result {
        std::panic::resume_unwind(err);
    }

    assert!(
        transition_state.missed_immediate_tasks() > missed_before,
        "the operation should exercise the forced immediate enqueue failure"
    );
    assert!(
        transition_state.compensation_scheduled_tasks() > compensation_before,
        "the operation should schedule automatic compensation backfill"
    );

    // Separate automatic discovery and queue latency from the metadata commit deadline.
    put_barrier.wait_until_paused().await;
    assert_eq!(backend.object_count().await, 1, "compensation should store the body before committing");
    put_barrier.release();
}

pub(super) async fn wait_for_delete_marker(ecstore: &Arc<ECStore>, bucket: &str, object: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Ok(info) = ecstore.get_object_info(bucket, object, &ObjectOptions::default()).await
            && info.delete_marker
        {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

pub(super) fn streaming_blob_from_bytes(data: &[u8]) -> StreamingBlob {
    let body = Bytes::copy_from_slice(data);
    StreamingBlob::wrap::<_, Infallible>(stream::once(async move { Ok(body) }))
}

pub(super) async fn read_object_bytes(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> Vec<u8> {
    let mut reader = (**ecstore)
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("Failed to read object");
    let mut buf = Vec::new();
    reader
        .stream
        .read_to_end(&mut buf)
        .await
        .expect("Failed to drain object reader");
    buf
}

pub(super) async fn live_object_version_count(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> usize {
    let versions = ecstore
        .clone()
        .list_object_versions(bucket, object, None, None, None, 1000)
        .await
        .expect("Failed to list object versions");

    versions
        .objects
        .iter()
        .filter(|info| info.name == object && !info.delete_marker)
        .count()
}

pub(super) async fn install_delete_replication_config(
    bucket: &str,
    delete_enabled: bool,
    replica_modifications: bool,
    required_tag: Option<(&str, &str)>,
) {
    use super::super::storage_api::test::{bucket::utils::serialize, get_global_bucket_metadata_sys, set_bucket_metadata};

    let sys = get_global_bucket_metadata_sys().expect("bucket metadata system should be initialized");
    let metadata = {
        let sys = sys.read().await;
        sys.get(bucket)
            .await
            .expect("bucket metadata should be cached before replication config injection")
    };
    let mut metadata = (*metadata).clone();
    let target = "arn:aws:s3:::target-bucket";
    let delete_status = if delete_enabled {
        Status::ENABLED.to_string()
    } else {
        Status::DISABLED.to_string()
    };

    metadata.versioning_config_xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
    metadata.versioning_config = Some(rustfs_gateway_types::persistence::PersistedVersioningConfiguration {
        status: Some("Enabled".to_owned()),
        ..Default::default()
    });
    let filter = required_tag.map(|(key, value)| PersistedReplicationFilter {
        tag: Some(PersistedReplicationTag {
            key: Some(key.to_string()),
            value: Some(value.to_string()),
        }),
        ..Default::default()
    });
    let source_selection_criteria = replica_modifications.then(|| PersistedSourceSelectionCriteria {
        replica_modifications: Some(PersistedReplicationStatus {
            status: Status::ENABLED.to_string(),
        }),
        sse_kms_encrypted_objects: None,
    });
    let replication_config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![PersistedReplicationRule {
            delete_marker_replication: Some(PersistedOptionalReplicationStatus {
                status: Some(Status::DISABLED.to_string()),
            }),
            delete_replication: Some(PersistedReplicationStatus { status: delete_status }),
            destination: PersistedReplicationDestination {
                bucket: target.to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter,
            id: Some("delete".to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria,
            status: Status::ENABLED.to_string(),
        }],
    };
    metadata.replication_config_xml = serialize(&replication_config).expect("replication test config should serialize");
    metadata.replication_config = Some(replication_config);
    set_bucket_metadata(bucket.to_string(), metadata)
        .await
        .expect("delete replication metadata should be installed for the request test");
}
