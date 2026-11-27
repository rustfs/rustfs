// Copyright 2026 RustFS Team
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

//! End-to-end `ECStore` multipart ETag-of-ETags and composite / full-object
//! checksum algebraic property tests (`rustfs/backlog#1151` `sec-11`).
//!
//! Exercises the real `ECStore::new_multipart_upload` -> `put_object_part` ->
//! `complete_multipart_upload` pipeline over an isolated local 4-drive erasure
//! store and verifies S3 ETag-of-ETags and checksum algebra directly against
//! independent reference digests.

#![recursion_limit = "256"]

mod storage_api;

use base64_simd::STANDARD as BASE64_STANDARD;
use md5::{Digest as Md5Digest, Md5};
use proptest::prelude::*;
use rustfs_rio::{Checksum, ChecksumType, HashReader};
use sha2::Sha256;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage_api::contract_compat::{
    CompletePart, ECStore, ObjectOptions, PutObjReader, StorageBucketOperations as _, StorageMultipartOperations as _,
};
use storage_api::pinned_erasure_fixtures::{
    Endpoint, EndpointServerPools, Endpoints, InstanceContext, MakeBucketOptions, PoolEndpoints, init_bucket_metadata_sys,
    init_local_disks_with_instance_ctx,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;

struct LocalFourDriveStore {
    _dirs: Vec<TempDir>,
    _cancel: tokio_util::sync::DropGuard,
    store: Arc<ECStore>,
}

impl LocalFourDriveStore {
    async fn new(bucket: &str) -> Self {
        let mut dirs = Vec::with_capacity(4);
        let mut endpoints = Vec::with_capacity(4);
        for disk in 0..4 {
            let dir = tempfile::tempdir().expect("create isolated multipart test drive");
            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("UTF-8 disk path")).expect("local disk endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk);
            dirs.push(dir);
            endpoints.push(endpoint);
        }
        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "multipart-etag-checksum-proptest".to_string(),
            platform: "test".to_string(),
        }]);
        let instance = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance, endpoint_pools.clone())
            .await
            .expect("initialize isolated 4-drive disks");
        let cancel = CancellationToken::new();
        let cancel_guard = cancel.clone().drop_guard();
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("local loopback address"),
            endpoint_pools,
            cancel,
            instance,
        )
        .await
        .expect("initialize isolated ECStore");
        init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create multipart proptest bucket");
        Self {
            _dirs: dirs,
            _cancel: cancel_guard,
            store,
        }
    }
}

fn reference_multipart_etag(parts: &[Vec<u8>]) -> String {
    let mut concat_raw_md5 = Vec::with_capacity(parts.len() * 16);
    for part in parts {
        concat_raw_md5.extend_from_slice(&Md5::digest(part));
    }
    let outer = Md5::digest(&concat_raw_md5);
    format!("{}-{}", faster_hex::hex_string(outer.as_slice()), parts.len())
}

fn reference_composite_sha256(parts: &[Vec<u8>]) -> String {
    let mut concat_raw = Vec::with_capacity(parts.len() * 32);
    for part in parts {
        concat_raw.extend_from_slice(&Sha256::digest(part));
    }
    let outer = Sha256::digest(&concat_raw);
    format!("{}-{}", BASE64_STANDARD.encode_to_string(outer.as_slice()), parts.len())
}

fn reference_full_object_crc32(parts: &[Vec<u8>]) -> String {
    let mut concat = Vec::new();
    for part in parts {
        concat.extend_from_slice(part);
    }
    Checksum::new_from_data(ChecksumType::CRC32, &concat)
        .expect("reference full-object CRC32")
        .encoded
}

async fn upload_and_complete(
    store: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    checksum_type: ChecksumType,
    parts: &[Vec<u8>],
) -> (String, bytes::Bytes) {
    let create_opts = ObjectOptions {
        want_checksum: Some(Checksum {
            checksum_type,
            ..Default::default()
        }),
        ..Default::default()
    };
    let upload = store
        .new_multipart_upload(bucket, object, &create_opts)
        .await
        .expect("new_multipart_upload must succeed");

    let mut completed_parts = Vec::with_capacity(parts.len());
    for (idx, payload) in parts.iter().enumerate() {
        let len = i64::try_from(payload.len()).expect("part length fits i64");
        let mut hash_reader =
            HashReader::from_stream(Cursor::new(payload.clone()), len, len, None, None, false).expect("construct HashReader");
        let part_cs = Checksum::new_from_data(checksum_type.base(), payload).expect("compute part checksum");
        hash_reader
            .add_non_trailing_checksum(Some(part_cs.clone()), false)
            .expect("attach part checksum");
        let mut reader = PutObjReader::new(hash_reader);
        let part_info = store
            .put_object_part(bucket, object, &upload.upload_id, idx + 1, &mut reader, &create_opts)
            .await
            .expect("put_object_part must succeed");
        let mut cp = CompletePart {
            part_num: part_info.part_num,
            etag: part_info.etag,
            ..Default::default()
        };
        match checksum_type.base() {
            ChecksumType::SHA256 => cp.checksum_sha256 = Some(part_cs.encoded),
            ChecksumType::CRC32 => cp.checksum_crc32 = Some(part_cs.encoded),
            other => panic!("unexpected checksum type {other:?}"),
        }
        completed_parts.push(cp);
    }

    let result = Arc::clone(store)
        .complete_multipart_upload(bucket, object, &upload.upload_id, completed_parts, &ObjectOptions::default())
        .await
        .expect("complete_multipart_upload must succeed");
    let etag = result.etag.expect("completed multipart object must carry ETag");
    let checksum_bytes = result
        .checksum
        .expect("completed multipart object must persist binary checksum record");
    (etag, checksum_bytes)
}

#[test]
fn ecstore_complete_multipart_single_part_proptest_preserves_etag_and_checksum_algebra() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let bucket = "mp-etag-checksum-single-proptest";
    let fixture = rt.block_on(LocalFourDriveStore::new(bucket));
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 16,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    let strategy = (prop::collection::vec(any::<u8>(), 0..=4096), any::<bool>());

    let counter = AtomicUsize::new(0);
    runner
        .run(&strategy, |(payload, use_full_crc32)| {
            let seq = counter.fetch_add(1, Ordering::Relaxed) + 1;
            let parts = vec![payload];
            let expected_etag = reference_multipart_etag(&parts);
            let object = format!("single-{seq}.bin");

            let (checksum_type, map_key, expected_cs, expected_type) = if use_full_crc32 {
                let mut ct = ChecksumType::CRC32;
                ct.merge(ChecksumType::FULL_OBJECT);
                (ct, "CRC32", reference_full_object_crc32(&parts), Some("FULL_OBJECT"))
            } else {
                (ChecksumType::SHA256, "SHA256", reference_composite_sha256(&parts), None)
            };

            let (actual_etag, checksum_bytes) =
                rt.block_on(upload_and_complete(&fixture.store, bucket, &object, checksum_type, &parts));

            let (map, is_multipart) = rustfs_rio::read_checksums(checksum_bytes.as_ref(), 0);
            prop_assert_eq!(actual_etag, expected_etag);
            prop_assert!(is_multipart, "stored checksum record must carry MULTIPART flag");
            prop_assert_eq!(map.get(map_key).map(String::as_str), Some(expected_cs.as_str()));
            prop_assert_eq!(map.get("x-amz-checksum-type").map(String::as_str), expected_type);
            Ok(())
        })
        .expect("single-part ECStore multipart ETag/checksum proptest failed");
}

#[test]
fn ecstore_complete_multipart_multi_part_proptest_preserves_etag_and_composite_and_full_object_checksums() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let bucket = "mp-etag-checksum-multi-proptest";
    let fixture = rt.block_on(LocalFourDriveStore::new(bucket));
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 4,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    // 2..=3 parts: non-final parts meet S3's 5 MiB minimum with randomized byte
    // patterns and extra bytes; the final part has arbitrary small length.
    let strategy = (
        prop::collection::vec((any::<u8>(), 0usize..=257usize), 1..=2),
        prop::collection::vec(any::<u8>(), 1..=1024),
        any::<bool>(),
    );

    let counter = AtomicUsize::new(0);
    runner
        .run(&strategy, |(leading_specs, tail_part, use_full_crc32)| {
            let seq = counter.fetch_add(1, Ordering::Relaxed) + 1;
            let mut parts: Vec<Vec<u8>> = leading_specs
                .into_iter()
                .enumerate()
                .map(|(i, (seed, extra))| {
                    let len = MIN_MULTIPART_PART_SIZE + extra;
                    let mut buf = vec![seed; len];
                    // Break homogeneity at head/middle/tail so ETag/CRC depends on exact byte order.
                    buf[0] = seed.wrapping_add(i as u8).wrapping_add(1);
                    buf[len / 2] = seed.wrapping_add(0x5a);
                    buf[len - 1] = seed.wrapping_sub(i as u8).wrapping_sub(3);
                    buf
                })
                .collect();
            parts.push(tail_part);

            let expected_etag = reference_multipart_etag(&parts);
            let object = format!("multi-{seq}.bin");
            let (checksum_type, map_key, expected_cs, expected_type) = if use_full_crc32 {
                let mut ct = ChecksumType::CRC32;
                ct.merge(ChecksumType::FULL_OBJECT);
                (ct, "CRC32", reference_full_object_crc32(&parts), Some("FULL_OBJECT"))
            } else {
                (ChecksumType::SHA256, "SHA256", reference_composite_sha256(&parts), None)
            };

            let (actual_etag, checksum_bytes) =
                rt.block_on(upload_and_complete(&fixture.store, bucket, &object, checksum_type, &parts));

            let (map, is_multipart) = rustfs_rio::read_checksums(checksum_bytes.as_ref(), 0);
            prop_assert_eq!(actual_etag, expected_etag);
            prop_assert!(is_multipart, "stored checksum record must carry MULTIPART flag");
            prop_assert_eq!(map.get(map_key).map(String::as_str), Some(expected_cs.as_str()));
            prop_assert_eq!(map.get("x-amz-checksum-type").map(String::as_str), expected_type);
            // Per-part checksum recovery must match each uploaded part's exact digest.
            for (idx, part) in parts.iter().enumerate() {
                let part_num = i32::try_from(idx + 1).expect("part number fits i32");
                let (part_map, _) = rustfs_rio::read_checksums(checksum_bytes.as_ref(), part_num);
                let expected_part = Checksum::new_from_data(checksum_type.base(), part)
                    .expect("compute expected part checksum")
                    .encoded;
                prop_assert_eq!(part_map.get(map_key).map(String::as_str), Some(expected_part.as_str()));
            }
            Ok(())
        })
        .expect("multi-part ECStore multipart ETag/checksum proptest failed");
}
