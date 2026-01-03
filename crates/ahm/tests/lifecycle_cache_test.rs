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

use heed::byteorder::BigEndian;
use heed::types::*;
use heed::{BoxedError, BytesDecode, BytesEncode, Database, DatabaseFlags, Env, EnvOpenOptions};
use rustfs_ahm::scanner::local_scan::{self, LocalObjectRecord, LocalScanOutcome};
use rustfs_ecstore::{
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{MakeBucketOptions, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader, StorageAPI},
};
use serial_test::serial;
use std::{
    borrow::Cow,
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
};
//use heed_traits::Comparator;
use time::OffsetDateTime;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();
static INIT: Once = Once::new();

static _LIFECYCLE_EXPIRY_CURRENT_DAYS: i32 = 1;
static _LIFECYCLE_EXPIRY_NONCURRENT_DAYS: i32 = 1;
static _LIFECYCLE_TRANSITION_CURRENT_DAYS: i32 = 1;
static _LIFECYCLE_TRANSITION_NONCURRENT_DAYS: i32 = 1;
static GLOBAL_LMDB_ENV: OnceLock<Env> = OnceLock::new();
static GLOBAL_LMDB_DB: OnceLock<Database<I64<BigEndian>, LifecycleContentCodec>> = OnceLock::new();

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
    // User home directory
    /*if let Ok(home_dir) = env::var("HOME").or_else(|_| env::var("USERPROFILE")) {
        let mut path = PathBuf::from(home_dir);
        path.push(format!(".{DEFAULT_LOG_FILENAME}"));
        path.push(DEFAULT_LOG_DIR);
        if ensure_directory_writable(&path) {
            //return path;
        }
    }*/
    let test_lmdb_lifecycle_dir = "/tmp/lmdb_lifecycle".to_string();
    let temp_dir = std::path::PathBuf::from(&test_lmdb_lifecycle_dir);
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.unwrap();
    let lmdb_env = unsafe { EnvOpenOptions::new().max_dbs(100).open(&test_lmdb_lifecycle_dir).unwrap() };
    let bucket_name = format!("test-lc-cache-{}", "00000");
    let mut wtxn = lmdb_env.write_txn().unwrap();
    let db = match lmdb_env
        .database_options()
        .name(&format!("bucket_{bucket_name}"))
        .types::<I64<BigEndian>, LifecycleContentCodec>()
        .flags(DatabaseFlags::DUP_SORT)
        //.dup_sort_comparator::<>()
        .create(&mut wtxn)
    {
        Ok(db) => db,
        Err(err) => {
            panic!("lmdb error: {err}");
        }
    };
    let _ = wtxn.commit();
    let _ = GLOBAL_LMDB_ENV.set(lmdb_env);
    let _ = GLOBAL_LMDB_DB.set(db);

    // Store in global once lock
    let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

    (disk_paths, ecstore)
}

/// Test helper: Create a test bucket
#[allow(dead_code)]
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

    println!("object_info1: {object_info:?}");

    info!("Uploaded test object: {}/{} ({} bytes)", bucket, object, object_info.size);
}

/// Test helper: Check if object exists
async fn object_exists(ecstore: &Arc<ECStore>, bucket: &str, object: &str) -> bool {
    match (**ecstore).get_object_info(bucket, object, &ObjectOptions::default()).await {
        Ok(info) => !info.delete_marker,
        Err(_) => false,
    }
}

fn ns_to_offset_datetime(ns: i128) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(ns).ok()
}

fn convert_record_to_object_info(record: &LocalObjectRecord) -> ObjectInfo {
    let usage = &record.usage;

    ObjectInfo {
        bucket: usage.bucket.clone(),
        name: usage.object.clone(),
        size: usage.total_size as i64,
        delete_marker: !usage.has_live_object && usage.delete_markers_count > 0,
        mod_time: usage.last_modified_ns.and_then(ns_to_offset_datetime),
        ..Default::default()
    }
}

#[allow(dead_code)]
fn to_object_info(
    bucket: &str,
    object: &str,
    total_size: i64,
    delete_marker: bool,
    mod_time: OffsetDateTime,
    version_id: &str,
) -> ObjectInfo {
    ObjectInfo {
        bucket: bucket.to_string(),
        name: object.to_string(),
        size: total_size,
        delete_marker,
        mod_time: Some(mod_time),
        version_id: Some(Uuid::parse_str(version_id).unwrap()),
        ..Default::default()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LifecycleType {
    ExpiryCurrent,
    ExpiryNoncurrent,
    TransitionCurrent,
    TransitionNoncurrent,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LifecycleContent {
    ver_no: u8,
    ver_id: String,
    mod_time: OffsetDateTime,
    type_: LifecycleType,
    object_name: String,
}

pub struct LifecycleContentCodec;

impl BytesEncode<'_> for LifecycleContentCodec {
    type EItem = LifecycleContent;

    fn bytes_encode(lcc: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let (ver_no_byte, ver_id_bytes, mod_timestamp_bytes, type_byte, object_name_bytes) = match lcc {
            LifecycleContent {
                ver_no,
                ver_id,
                mod_time,
                type_: LifecycleType::ExpiryCurrent,
                object_name,
            } => (
                ver_no,
                ver_id.clone().into_bytes(),
                mod_time.unix_timestamp().to_be_bytes(),
                0,
                object_name.clone().into_bytes(),
            ),
            LifecycleContent {
                ver_no,
                ver_id,
                mod_time,
                type_: LifecycleType::ExpiryNoncurrent,
                object_name,
            } => (
                ver_no,
                ver_id.clone().into_bytes(),
                mod_time.unix_timestamp().to_be_bytes(),
                1,
                object_name.clone().into_bytes(),
            ),
            LifecycleContent {
                ver_no,
                ver_id,
                mod_time,
                type_: LifecycleType::TransitionCurrent,
                object_name,
            } => (
                ver_no,
                ver_id.clone().into_bytes(),
                mod_time.unix_timestamp().to_be_bytes(),
                2,
                object_name.clone().into_bytes(),
            ),
            LifecycleContent {
                ver_no,
                ver_id,
                mod_time,
                type_: LifecycleType::TransitionNoncurrent,
                object_name,
            } => (
                ver_no,
                ver_id.clone().into_bytes(),
                mod_time.unix_timestamp().to_be_bytes(),
                3,
                object_name.clone().into_bytes(),
            ),
        };

        let mut output = Vec::<u8>::new();
        output.push(*ver_no_byte);
        output.extend_from_slice(&ver_id_bytes);
        output.extend_from_slice(&mod_timestamp_bytes);
        output.push(type_byte);
        output.extend_from_slice(&object_name_bytes);
        Ok(Cow::Owned(output))
    }
}

impl<'a> BytesDecode<'a> for LifecycleContentCodec {
    type DItem = LifecycleContent;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        use std::mem::size_of;

        let ver_no = match bytes.get(..size_of::<u8>()) {
            Some(bytes) => bytes.try_into().map(u8::from_be_bytes).unwrap(),
            None => return Err("invalid LifecycleContent: cannot extract ver_no".into()),
        };

        let ver_id = match bytes.get(size_of::<u8>()..(36 + 1)) {
            Some(bytes) => unsafe { std::str::from_utf8_unchecked(bytes).to_string() },
            None => return Err("invalid LifecycleContent: cannot extract ver_id".into()),
        };

        let mod_timestamp = match bytes.get((36 + 1)..(size_of::<i64>() + 36 + 1)) {
            Some(bytes) => bytes.try_into().map(i64::from_be_bytes).unwrap(),
            None => return Err("invalid LifecycleContent: cannot extract mod_time timestamp".into()),
        };

        let type_ = match bytes.get(size_of::<i64>() + 36 + 1) {
            Some(&0) => LifecycleType::ExpiryCurrent,
            Some(&1) => LifecycleType::ExpiryNoncurrent,
            Some(&2) => LifecycleType::TransitionCurrent,
            Some(&3) => LifecycleType::TransitionNoncurrent,
            Some(_) => return Err("invalid LifecycleContent: invalid LifecycleType".into()),
            None => return Err("invalid LifecycleContent: cannot extract LifecycleType".into()),
        };

        let object_name = match bytes.get((size_of::<i64>() + 36 + 1 + 1)..) {
            Some(bytes) => unsafe { std::str::from_utf8_unchecked(bytes).to_string() },
            None => return Err("invalid LifecycleContent: cannot extract object_name".into()),
        };

        Ok(LifecycleContent {
            ver_no,
            ver_id,
            mod_time: OffsetDateTime::from_unix_timestamp(mod_timestamp).unwrap(),
            type_,
            object_name,
        })
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

        let records = match bucket_objects_map.get(&bucket_name) {
            Some(records) => records,
            None => {
                debug!("No local snapshot entries found for bucket {}; skipping lifecycle/integrity", bucket_name);
                &vec![]
            }
        };

        if let Some(lmdb_env) = GLOBAL_LMDB_ENV.get()
            && let Some(lmdb) = GLOBAL_LMDB_DB.get()
        {
            let mut wtxn = lmdb_env.write_txn().unwrap();

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

                let object_info = convert_record_to_object_info(record);
                println!("object_info2: {object_info:?}");
                let mod_time = object_info.mod_time.unwrap_or(OffsetDateTime::now_utc());
                let expiry_time = rustfs_ecstore::bucket::lifecycle::lifecycle::expected_expiry_time(mod_time, 1);

                let version_id = if let Some(version_id) = object_info.version_id {
                    version_id.to_string()
                } else {
                    "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz".to_string()
                };

                lmdb.put(
                    &mut wtxn,
                    &expiry_time.unix_timestamp(),
                    &LifecycleContent {
                        ver_no: 0,
                        ver_id: version_id,
                        mod_time,
                        type_: LifecycleType::TransitionNoncurrent,
                        object_name: object_info.name,
                    },
                )
                .unwrap();
            }

            wtxn.commit().unwrap();

            let mut wtxn = lmdb_env.write_txn().unwrap();
            let iter = lmdb.iter_mut(&mut wtxn).unwrap();
            //let _ = unsafe { iter.del_current().unwrap() };
            for row in iter {
                if let Ok(ref elm) = row {
                    let LifecycleContent {
                        ver_no,
                        ver_id,
                        mod_time,
                        type_,
                        object_name,
                    } = &elm.1;
                    println!("cache row:{ver_no} {ver_id} {mod_time} {type_:?} {object_name}");
                }
                println!("row:{row:?}");
            }
            //drop(iter);
            wtxn.commit().unwrap();
        }

        println!("Lifecycle cache test completed");
    }
}
