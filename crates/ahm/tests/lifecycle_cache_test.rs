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

use rustfs_ahm::scanner::Scanner;
use rustfs_ahm::scanner::local_scan::{self, LocalObjectRecord, LocalScanOutcome};
use rustfs_ecstore::{
    bucket::metadata_sys,
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader, StorageAPI},
};
use serial_test::serial;
use std::sync::Once;
use std::sync::OnceLock;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::fs;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing::{debug, info};
use heed::{BoxedError, BytesDecode, BytesEncode, Database, EnvOpenOptions, DatabaseFlags};
use heed::types::*;
//use heed_traits::Comparator;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static INIT: Once = Once::new();

static LIFECYCLE_EXPIRY_CURRENT_DAYS: i32 = 1;
static LIFECYCLE_EXPIRY_NONCURRENT_DAYS: i32 = 1;
static LIFECYCLE_TRANSITION_CURRENT_DAYS: i32 = 1;
static LIFECYCLE_TRANSITION_NONCURRENT_DAYS: i32 = 1;
static GLOBAL_LMDB_ENV: OnceLock<EnvOpenOptions> = OnceLock::new();
static GLOBAL_LMDB_DB: OnceLock<Database<Str, LifecycleContentCodec>> = OnceLock::new();

fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}

/// Test helper: Create test environment with ECStore
async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_tracing();

    // Fast path: already initialized, just clone and return
    if let Some((paths, ecstore)) = GLOBAL_ENV.get() {
        return (paths.clone(), ecstore.clone());
    }

    // create temp dir as 4 disks with unique base dir
    let test_base_dir = format!("/tmp/rustfs_ahm_lifecyclecache_test_{}", uuid::Uuid::new_v4());
    let temp_dir = std::path::PathBuf::from(&test_base_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();

    // create 4 disk dirs
    let disk_paths = vec![
        temp_dir.join("disk1"),
        temp_dir.join("disk2"),
        temp_dir.join("disk3"),
        temp_dir.join("disk4"),
    ];

    for disk_path in &disk_paths {
        fs::create_dir_all(disk_path).await.unwrap();
    }

    // create EndpointServerPools
    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        // set correct index
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

    // format disks (only first time)
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

    // create ECStore with dynamic port 0 (let OS assign) or fixed 9002 if free
    let port = 9002; // for simplicity
    let server_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    // init bucket metadata system
    let buckets_list = ecstore
        .list_bucket(&rustfs_ecstore::store_api::BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    //lmdb env
    let lmdb_env = unsafe {
        EnvOpenOptions::new()::open("lifecycle-db")?
    };
    let mut wtxn = lmdb_env.write_txn()?;
    let db = lmdb_env
        .database_options()
        .types::<Str, LifecycleContentCodec>()
        .flags(DatabaseFlags::DUP_SORT)
        //.dup_sort_comparator::<>()
        .create(&mut wtxn)?;
    wtxn.commit();
    let _ = GLOBAL_LMDB_ENV.set(lmdb_env);
    let _ = GLOBAL_LMDB_DB.set(db);

    // Store in global once lock
    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

    (disk_paths, ecstore)
}

/// Test helper: Create a test bucket
async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(bucket_name, &Default::default())
        .await
        .expect("Failed to create test bucket");
    info!("Created test bucket: {}", bucket_name);
}

/// Test helper: Create a test lock bucket
async fn create_test_lock_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
    (**ecstore)
        .make_bucket(
            bucket_name,
            &MakeBucketOptions {
                lock_enabled: true,
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create test bucket");
    info!("Created test bucket: {}", bucket_name);
}

/// Test helper: Upload test object
async fn upload_test_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) {
    let mut reader = PutObjReader::from_vec(data.to_vec());
    let object_info = (**ecstore)
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload test object");

    info!("Uploaded test object: {}/{} ({} bytes)", bucket, object, object_info.size);
}

/// Test helper: Check if object exists
async fn object_exists(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    match (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        Ok(info) => !info.delete_marker,
        Err(_) => false,
    }
}

enum LifecycleType {
    ExpiryCurrent,
    ExpiryNoncurrent,
    TransitionCurrent,
    TransitionNoncurrent,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LifecycleContent {
    ver_id: String,
    mod_time: OffsetDatetime,
    tier: String,
    type_: LifecycleType,
}

pub struct LifecycleContentCodec;

impl<'a> BytesEncode<'a> for LifecycleContentCodec {
    type EItem = LifecycleContent;

    /// Encodes the u32 timestamp in big endian followed by the log level with a single byte.
    fn bytes_encode(lcc: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let (ver_id_bytes, timestamp_bytes, tier_bytes, lifecycle_type) = match lcc {
            LifecycleContent { ver_id, mod_time, tier, LifecycleType::ExpiryCurrent } => (timestamp.to_be_bytes(), 0),
            LifecycleContent { ver_id, mod_time, tier, LifecycleType::ExpiryNoncurrent } => (timestamp.to_be_bytes(), 1),
            LifecycleContent { ver_id, mod_time, tier, LifecycleType::TransitionCurrent } => (timestamp.to_be_bytes(), 2),
            LifecycleContent { ver_id, mod_time, tier, LifecycleType::TransitionNoncurrent } => (timestamp.to_be_bytes(), 3),
        };

        let mut output = Vec::new();
        output.extend_from_slice(&timestamp_bytes);
        output.push(level_byte);
        Ok(Cow::Owned(output))
    }
}

impl<'a> BytesDecode<'a> for LifecycleContentCodec {
    type DItem = LifecycleContent;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        use std::mem::size_of;

        let timestamp = match bytes.get(..size_of::<u32>()) {
            Some(bytes) => bytes.try_into().map(u32::from_be_bytes).unwrap(),
            None => return Err("invalid log key: cannot extract timestamp".into()),
        };

        let type_ = match bytes.get(size_of::<u32>()) {
            Some(&0) => LifecycleType::ExpiryCurrent,
            Some(&1) => LifecycleType::ExpiryNoncurrent,
            Some(&2) => LifecycleType::TransitionCurrent,
            Some(&3) => LifecycleType::TransitionNoncurrent,
            Some(_) => return Err("invalid log key: invalid log level".into()),
            None => return Err("invalid log key: cannot extract log level".into()),
        };

        Ok(LifecycleContent { ver_id, mod_time, tier, type_ })
    }
}

mod serial_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    //#[ignore]
    async fn test_lifecycle_chche_build() {
        let (_disk_paths, ecstore) = setup_test_env().await;

        // Create test bucket and object
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let bucket_name = format!("test-lc-cache-{}", &suffix[..8]);
        let object_name = "test/object.txt"; // Match the lifecycle rule prefix "test/"
        let test_data = b"Hello, this is test data for lifecycle expiry!";

        create_test_lock_bucket(&ecstore, bucket_name.as_str()).await;
        upload_test_object(&ecstore, bucket_name.as_str(), object_name, test_data).await;

        // Verify object exists initially
        assert!(object_exists(&ecstore, bucket_name.as_str(), object_name).await);
        println!("✅ Object exists before lifecycle processing");

        let scan_outcome = match local_scan::scan_and_persist_local_usage(ecstore.clone()).await {
            Ok(outcome) => outcome,
            Err(err) => {
                warn!("Local usage scan failed: {}", err);
                LocalScanOutcome::default()
            }
        };
        let bucket_objects_map = &scan_outcome.bucket_objects;

        let records = match bucket_objects_map.get(bucket_name) {
            Some(records) => records,
            None => {
                debug!(
                    "No local snapshot entries found for bucket {}; skipping lifecycle/integrity",
                    bucket_name
                );
            }
        };

        if let Some(lmdb_env) = GLOBAL_LMDB_ENV.get() {
            if let Some(lmdb) = GLOBAL_LMDB_DB.get() {
                let mut wtxn = lmdb_env.write_txn()?;

                /*if let Ok((lc_config, _)) = rustfs_ecstore::bucket::metadata_sys::get_lifecycle_config(bucket_name.as_str()).await {
                    if let Ok(object_info) = ecstore
                        .get_object_info(bucket_name.as_str(), object_name, &rustfs_ecstore::store_api::ObjectOptions::default())
                        .await
                    {
                        let event = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::eval_action_from_lifecycle(
                            &lc_config,
                            None,
                            None,
                            &object_info,
                        )
                        .await;

                        rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
                            ecstore.clone(),
                            &object_info,
                            &event,
                            &rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
                        )
                        .await;

                        expired = wait_for_object_absence(&ecstore, bucket_name.as_str(), object_name, Duration::from_secs(2)).await;
                    }
                }*/

                for record in records {
                    if !record.usage.has_live_object {
                        continue;
                    }

                    let object_info = Scanner::convert_record_to_object_info(record);
                    rustfs_ecstore::bucket::lifecycle::lifecycle::expected_expiry_time(object_info.mod_time, 1);
                }

                lmdb.put(
                    &mut wtxn,
                    "123143242",
                    &LifecycleContent { timestamp: 1608326232, level: Level::Debug },
                )?;
                wtxn.commit()?;

                let rtxn = lmdb_env.read_txn()?;
                let _ = lmdb.get(&rtxn, "123143242")?;
                //rtxn.commit()?;

                let mut wtxn = lmdb_env.write_txn()?;
                let mut iter = lmdb.iter_mut(&mut wtxn)?;
                let _ = iter.next().transpose()?;
                let _ = unsafe { iter.del_curent()? };
                let _ = unsafe { iter.put_curent(
                    "123143242",
                    &LifecycleContent { timestamp: 1608326232, level: Level::Debug },
                )? };
                let _ = lmdb.dalete(&mut wtxn, "123143242")?;
                wtxn.commit()?;
            }
        }
        
        println!("Lifecycle expiry basic test completed");
    }
}
