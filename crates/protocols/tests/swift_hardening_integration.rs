// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end Swift protocol hardening integration tests.
//!
//! Covers:
//! - Unknown-length (`-1`) and lying `Content-Length` upload size bounds (`MAX_OBJECT_SIZE`).
//! - Container byte & object count quotas enforced across regular PUT, concurrent PUT races,
//!   bulk archive extraction, FormPost, SLO PUT, DLO registration, and server-side COPY.
//! - Bulk archive compression-bomb rejection and FormPost open-redirect / binary-safety guards.

#![recursion_limit = "256"]
#![cfg(feature = "swift")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use rustfs_credentials::Credentials;
use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
use rustfs_protocols::swift::bulk::{
    ArchiveFormat, BulkDeleteResponse, BulkExtractLimits, BulkExtractResponse, handle_bulk_delete, handle_bulk_extract,
    handle_bulk_extract_with_limits,
};
use rustfs_protocols::swift::container::{ContainerMapper, get_container_metadata, list_objects, update_container_metadata};
use rustfs_protocols::swift::dlo::handle_dlo_register;
use rustfs_protocols::swift::formpost::{generate_signature, handle_formpost};
use rustfs_protocols::swift::handler::SwiftRequestBody;
use rustfs_protocols::swift::object::{
    copy_object, delete_object, get_object, head_object, put_object, put_object_with_max_size, put_object_with_metadata,
};
use rustfs_protocols::swift::slo::handle_slo_put;
use rustfs_protocols::swift::{MetadataUpdate, SwiftError};
use rustfs_test_utils::TestECStoreEnv;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Barrier;

mod ecstore_test_compat;
use ecstore_test_compat::{invalidate_data_usage_snapshot_cache, store_data_usage_in_backend};

fn keystone_credentials(project_id: &str) -> Credentials {
    let mut claims = HashMap::new();
    claims.insert("keystone_project_id".to_string(), json!(project_id));
    claims.insert("keystone_roles".to_string(), json!(["member"]));

    Credentials {
        access_key: format!("keystone:{project_id}"),
        claims: Some(claims),
        ..Default::default()
    }
}

async fn seed_empty_usage_snapshot(env: &TestECStoreEnv, buckets: &[String]) {
    let mut info = DataUsageInfo {
        last_update: Some(SystemTime::now()),
        usage_snapshot_complete: true,
        buckets_count: buckets.len() as u64,
        ..Default::default()
    };
    for bucket in buckets {
        info.buckets_usage.insert(bucket.clone(), BucketUsageInfo::default());
        info.bucket_sizes.insert(bucket.clone(), 0);
    }
    info.calculate_totals();
    store_data_usage_in_backend(info, env.ecstore.clone())
        .await
        .expect("seed data usage snapshot");
    invalidate_data_usage_snapshot_cache().await;
}

fn headers_with_len(len: usize) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", len.to_string().parse().expect("valid content-length"));
    headers
}

fn boxed_body(bytes: Vec<u8>) -> SwiftRequestBody {
    UnsyncBoxBody::new(Full::new(Bytes::from(bytes)).map_err(|never: std::convert::Infallible| match never {}))
}

async fn make_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut builder = tokio_tar::Builder::new(buf);
    for &(name, data) in files {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, Cursor::new(data))
            .await
            .expect("append tar entry");
    }
    builder.into_inner().await.expect("finish tar").into_inner()
}

fn build_formpost_multipart(boundary: &str, fields: &[(&str, &str)], files: &[(&str, &str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        out.extend_from_slice(format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes());
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    for (name, filename, data) in files {
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        out.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    out
}

/// All scenarios share ONE `TestECStoreEnv` because Swift resolves the ambient
/// process-wide object store handle. Spawns the async body onto a 16 MiB Tokio
/// worker thread so libtest's 2 MiB calling thread never polls the ECStore
/// state machine in unoptimized debug builds.
#[test]
fn swift_hardening_bounds_and_quotas_end_to_end() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build multi-thread test runtime");

    let handle = runtime.spawn(Box::pin(async {
        let env = TestECStoreEnv::builder().prefix("swift_hardening").build().await;
        let project_id = "swifthardenproj";
        let account = format!("AUTH_{project_id}");
        let creds = keystone_credentials(project_id);
        let mapper = ContainerMapper::default();

        let containers = [
            "size-limits",
            "quota-put",
            "quota-race",
            "quota-bulk",
            "quota-formpost",
            "quota-slo-tight",
            "quota-slo-ok",
            "quota-dlo",
            "quota-copy-src",
            "quota-copy-dst",
        ];
        let mut buckets = Vec::with_capacity(containers.len());
        for c in containers {
            let bucket = mapper.swift_to_s3_bucket(c, project_id);
            env.make_bucket(&bucket, false).await;
            buckets.push(bucket);
        }
        seed_empty_usage_snapshot(&env, &buckets).await;

        Box::pin(scenario_unknown_length_and_negative_content_length(&account, &creds)).await;
        Box::pin(scenario_regular_put_and_delete_quotas(&account, &creds)).await;
        Box::pin(scenario_concurrent_put_quota_race(&account, &creds)).await;
        Box::pin(scenario_bulk_extract_quota_and_compression_bomb(&account, &creds)).await;
        Box::pin(scenario_bulk_delete_functional(&account, &creds)).await;
        Box::pin(scenario_formpost_quota_binary_and_redirect(&account, &creds)).await;
        Box::pin(scenario_slo_put_atomic_two_object_quota(&account, &creds)).await;
        Box::pin(scenario_dlo_register_quota(&account, &creds)).await;
        Box::pin(scenario_copy_object_destination_quota(&account, &creds)).await;
    }));
    runtime.block_on(handle).expect("hardening integration task panicked");
}

async fn scenario_unknown_length_and_negative_content_length(account: &str, creds: &Credentials) {
    // Negative Content-Length header must be rejected with 400 BadRequest.
    let mut neg_headers = HeaderMap::new();
    neg_headers.insert("content-length", "-1".parse().expect("header"));
    let err = put_object(account, "size-limits", "neg.bin", creds, Cursor::new(b"abc".to_vec()), &neg_headers)
        .await
        .expect_err("negative Content-Length must fail");
    assert!(matches!(err, SwiftError::BadRequest(_)));

    // Unknown-length upload within max_object_size succeeds.
    let exact = vec![0x5Au8; 1024];
    put_object_with_max_size(
        account,
        "size-limits",
        "exact.bin",
        creds,
        Cursor::new(exact.clone()),
        &HeaderMap::new(),
        1024,
    )
    .await
    .expect("unknown-length upload at limit must succeed");

    let mut reader = get_object(account, "size-limits", "exact.bin", creds, None)
        .await
        .expect("read back exact.bin");
    let mut read_back = Vec::new();
    reader.stream.read_to_end(&mut read_back).await.expect("read stream");
    assert_eq!(read_back, exact);

    // Unknown-length upload exceeding max_object_size aborts with 413 and does NOT commit.
    let oversized = vec![0xA5u8; 2048];
    let err = put_object_with_max_size(
        account,
        "size-limits",
        "too-big-unknown.bin",
        creds,
        Cursor::new(oversized.clone()),
        &HeaderMap::new(),
        1024,
    )
    .await
    .expect_err("unknown-length upload exceeding max size must fail closed");
    assert!(
        matches!(err, SwiftError::RequestEntityTooLarge(_)),
        "expected RequestEntityTooLarge, got {err:?}"
    );
    assert!(
        matches!(
            head_object(account, "size-limits", "too-big-unknown.bin", creds).await,
            Err(SwiftError::NotFound(_))
        ),
        "aborted oversized upload must not commit an object"
    );

    // Lying declared Content-Length (512) with stream larger than max_object_size (1024) also aborts with 413.
    let err = put_object_with_max_size(
        account,
        "size-limits",
        "lying-len.bin",
        creds,
        Cursor::new(oversized),
        &headers_with_len(512),
        1024,
    )
    .await
    .expect_err("body exceeding max size despite smaller declared Content-Length must fail");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
    assert!(matches!(
        head_object(account, "size-limits", "lying-len.bin", creds).await,
        Err(SwiftError::NotFound(_))
    ));
}

async fn scenario_regular_put_and_delete_quotas(account: &str, creds: &Credentials) {
    update_container_metadata(
        account,
        "quota-put",
        creds,
        MetadataUpdate::default().set("quota-bytes", "1000").set("quota-count", "3"),
    )
    .await
    .expect("set quota-put metadata");

    // Byte quota rejects missing Content-Length with 411 LengthRequired.
    let err = put_object(account, "quota-put", "no-len.bin", creds, Cursor::new(vec![1u8; 10]), &HeaderMap::new())
        .await
        .expect_err("byte quota requires Content-Length");
    assert!(matches!(err, SwiftError::LengthRequired(_)));

    // 600B succeeds and updates in-memory usage overlay immediately.
    put_object(account, "quota-put", "o1.bin", creds, Cursor::new(vec![1u8; 600]), &headers_with_len(600))
        .await
        .expect("600B upload fits in 1000B quota");
    let meta = get_container_metadata(account, "quota-put", creds)
        .await
        .expect("container usage reflects commit immediately");
    assert_eq!(meta.bytes_used, 600);
    assert_eq!(meta.object_count, 1);

    // Second 600B exceeds 1000B byte quota.
    let err = put_object(
        account,
        "quota-put",
        "o2-big.bin",
        creds,
        Cursor::new(vec![2u8; 600]),
        &headers_with_len(600),
    )
    .await
    .expect_err("1200B total must exceed 1000B byte quota");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

    // Fill to 3 objects (600 + 200 + 100 = 900B).
    put_object(account, "quota-put", "o2.bin", creds, Cursor::new(vec![2u8; 200]), &headers_with_len(200))
        .await
        .expect("o2 fits");
    put_object(account, "quota-put", "o3.bin", creds, Cursor::new(vec![3u8; 100]), &headers_with_len(100))
        .await
        .expect("o3 fits");

    // 4th object exceeds quota-count=3 even though bytes (910) <= 1000.
    let err = put_object(account, "quota-put", "o4.bin", creds, Cursor::new(vec![4u8; 10]), &headers_with_len(10))
        .await
        .expect_err("4th object must exceed count quota 3");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

    // Deleting o3 frees count and bytes immediately so a new object can land.
    delete_object(account, "quota-put", "o3.bin", creds).await.expect("delete o3");
    put_object(account, "quota-put", "o4.bin", creds, Cursor::new(vec![4u8; 150]), &headers_with_len(150))
        .await
        .expect("upload after delete must succeed");
}

async fn scenario_concurrent_put_quota_race(account: &str, creds: &Credentials) {
    update_container_metadata(account, "quota-race", creds, MetadataUpdate::default().set("quota-bytes", "1000"))
        .await
        .expect("set quota-race metadata");

    let concurrency = 6usize;
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for idx in 0..concurrency {
        let barrier = Arc::clone(&barrier);
        let account = account.to_string();
        let creds = creds.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            put_object(
                &account,
                "quota-race",
                &format!("race-{idx}.bin"),
                &creds,
                Cursor::new(vec![idx as u8; 600]),
                &headers_with_len(600),
            )
            .await
        }));
    }

    let mut ok_count = 0usize;
    let mut quota_rejected = 0usize;
    for handle in handles {
        match handle.await.expect("worker task") {
            Ok(_) => ok_count += 1,
            Err(SwiftError::RequestEntityTooLarge(_)) => quota_rejected += 1,
            Err(other) => panic!("unexpected error in concurrent quota race: {other:?}"),
        }
    }

    assert_eq!(ok_count, 1, "at most one 600B upload may commit under a 1000B quota");
    assert_eq!(quota_rejected, concurrency - 1);

    let objects = list_objects(account, "quota-race", creds, None, None, None, None)
        .await
        .expect("list quota-race");
    assert_eq!(objects.len(), 1, "committed objects must match admitted count");
}

async fn scenario_bulk_extract_quota_and_compression_bomb(account: &str, creds: &Credentials) {
    update_container_metadata(account, "quota-bulk", creds, MetadataUpdate::default().set("quota-bytes", "500"))
        .await
        .expect("set quota-bulk metadata");

    let tar_bytes = make_tar(&[("first.bin", &[b'a'; 300]), ("second.bin", &[b'b'; 300])]).await;
    let resp = handle_bulk_extract(account, "quota-bulk", ArchiveFormat::Tar, tar_bytes, creds)
        .await
        .expect("bulk extract returns JSON summary");
    let body = resp.into_body().collect().await.expect("collect resp").to_bytes();
    let parsed: BulkExtractResponse = serde_json::from_slice(&body).expect("parse bulk extract json");
    assert_eq!(parsed.number_files_created, 1, "only the first 300B file fits in 500B quota");
    assert_eq!(parsed.errors.len(), 1, "second 300B file must be rejected by container quota");
    assert!(head_object(account, "quota-bulk", "first.bin", creds).await.is_ok());
    assert!(matches!(
        head_object(account, "quota-bulk", "second.bin", creds).await,
        Err(SwiftError::NotFound(_))
    ));

    // Gzip bomb fails closed with 413 at the streaming decompression cap.
    let bomb_tar = make_tar(&[("bomb.bin", &vec![0u8; 2 * 1024 * 1024])]).await;
    let mut encoder = async_compression::tokio::write::GzipEncoder::new(Cursor::new(Vec::new()));
    encoder.write_all(&bomb_tar).await.expect("write bomb");
    encoder.shutdown().await.expect("finish gzip");
    let gz_bomb = encoder.into_inner().into_inner();

    let err = handle_bulk_extract_with_limits(
        account,
        "quota-bulk",
        ArchiveFormat::TarGz,
        gz_bomb,
        creds,
        BulkExtractLimits {
            max_archive_bytes: 1024 * 1024,
            max_total_decompressed_bytes: 32 * 1024,
            max_entry_bytes: 32 * 1024,
            max_files: 10,
            max_total_entries: 20,
            max_errors: 10,
        },
    )
    .await
    .expect_err("gzip bomb must be rejected with 413");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
}

async fn scenario_bulk_delete_functional(account: &str, creds: &Credentials) {
    let resp = handle_bulk_delete(account, "/quota-bulk/first.bin\n/quota-bulk/missing.bin\n".to_string(), creds)
        .await
        .expect("bulk delete succeeds");
    let body = resp.into_body().collect().await.expect("collect resp").to_bytes();
    let parsed: BulkDeleteResponse = serde_json::from_slice(&body).expect("parse bulk delete json");
    assert_eq!(parsed.number_deleted, 2); // Swift delete_object is idempotent for missing objects
    assert!(parsed.errors.is_empty());
    assert!(matches!(
        head_object(account, "quota-bulk", "first.bin", creds).await,
        Err(SwiftError::NotFound(_))
    ));
}

async fn scenario_formpost_quota_binary_and_redirect(account: &str, creds: &Credentials) {
    update_container_metadata(account, "quota-formpost", creds, MetadataUpdate::default().set("quota-bytes", "500"))
        .await
        .expect("set quota-formpost metadata");

    let path = format!("/v1/{account}/quota-formpost");
    let signed_redirect = "https://portal.example.com/upload-result";
    let key = "formpost-signing-secret";
    let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;
    let sig = generate_signature(&path, signed_redirect, 2048, 2, expires, key).expect("sign formpost");
    let boundary = "----HardeningFormPost";
    let content_type = format!("multipart/form-data; boundary={boundary}");

    // Over-quota upload (600B > 500B) fails and redirects ONLY to signed origin, ignoring cross-origin redirect_error.
    let over_body = build_formpost_multipart(
        boundary,
        &[
            ("redirect", signed_redirect),
            ("redirect_error", "https://attacker.invalid/phish"),
            ("max_file_size", "2048"),
            ("max_file_count", "2"),
            ("expires", &expires.to_string()),
            ("signature", &sig),
        ],
        &[("file", "oversized.bin", &[0xEEu8; 600])],
    );
    let resp = handle_formpost(account, "quota-formpost", &path, &content_type, over_body, key, creds)
        .await
        .expect("authenticated FormPost returns 303 on upload error");
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    let location = resp.headers().get("location").and_then(|v| v.to_str().ok()).unwrap();
    assert!(
        location.starts_with("https://portal.example.com/upload-result?status=400&"),
        "cross-origin redirect_error must be ignored, got {location}"
    );
    assert!(matches!(
        head_object(account, "quota-formpost", "oversized.bin", creds).await,
        Err(SwiftError::NotFound(_))
    ));

    // Within-quota binary payload (non-UTF-8 bytes) succeeds and round-trips losslessly.
    let binary_payload: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x80, b'\r', b'\n', 0x01, 0xAB, 0xCD, 0x99];
    let ok_body = build_formpost_multipart(
        boundary,
        &[
            ("redirect", signed_redirect),
            ("max_file_size", "2048"),
            ("max_file_count", "2"),
            ("expires", &expires.to_string()),
            ("signature", &sig),
        ],
        &[("file", "raw.bin", &binary_payload)],
    );
    let resp = handle_formpost(account, "quota-formpost", &path, &content_type, ok_body, key, creds)
        .await
        .expect("valid FormPost succeeds");
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    let location = resp.headers().get("location").and_then(|v| v.to_str().ok()).unwrap();
    assert_eq!(location, "https://portal.example.com/upload-result?status=201&message=Created");

    let mut reader = get_object(account, "quota-formpost", "raw.bin", creds, None)
        .await
        .expect("read uploaded binary");
    let mut read_back = Vec::new();
    reader.stream.read_to_end(&mut read_back).await.expect("read stream");
    assert_eq!(read_back, binary_payload);
}

async fn scenario_slo_put_atomic_two_object_quota(account: &str, creds: &Credentials) {
    // Upload one valid segment in quota-copy-src.
    let seg_data = b"segment-payload-1".to_vec();
    let seg_etag = put_object(
        account,
        "quota-copy-src",
        "seg1",
        creds,
        Cursor::new(seg_data.clone()),
        &headers_with_len(seg_data.len()),
    )
    .await
    .expect("upload SLO segment");

    let manifest_json = serde_json::to_vec(&json!({
        "segments": [{
            "path": "/quota-copy-src/seg1",
            "size_bytes": seg_data.len(),
            "etag": seg_etag.trim_matches('"')
        }]
    }))
    .expect("serialize manifest");

    // Container with quota-count=1 must reject SLO PUT (which writes 2 objects: manifest + marker)
    // BEFORE creating either object.
    update_container_metadata(account, "quota-slo-tight", creds, MetadataUpdate::default().set("quota-count", "1"))
        .await
        .expect("set quota-slo-tight metadata");

    let err = handle_slo_put(
        account,
        "quota-slo-tight",
        "combo",
        boxed_body(manifest_json.clone()),
        &headers_with_len(manifest_json.len()),
        &Some(creds.clone()),
    )
    .await
    .expect_err("SLO PUT requiring 2 objects must fail under quota-count=1");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
    assert!(
        matches!(
            head_object(account, "quota-slo-tight", "combo.slo-manifest", creds).await,
            Err(SwiftError::NotFound(_))
        ),
        "rejected SLO PUT must not leave behind an orphan .slo-manifest"
    );

    // Container with quota-count=2 allows the SLO PUT (manifest + marker).
    update_container_metadata(
        account,
        "quota-slo-ok",
        creds,
        MetadataUpdate::default().set("quota-count", "2").set("quota-bytes", "10000"),
    )
    .await
    .expect("set quota-slo-ok metadata");

    let resp = handle_slo_put(
        account,
        "quota-slo-ok",
        "combo",
        boxed_body(manifest_json.clone()),
        &headers_with_len(manifest_json.len()),
        &Some(creds.clone()),
    )
    .await
    .expect("SLO PUT with room for 2 objects must succeed");
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(
        head_object(account, "quota-slo-ok", "combo.slo-manifest", creds)
            .await
            .is_ok()
    );
    assert!(head_object(account, "quota-slo-ok", "combo", creds).await.is_ok());
}

async fn scenario_dlo_register_quota(account: &str, creds: &Credentials) {
    update_container_metadata(account, "quota-dlo", creds, MetadataUpdate::default().set("quota-count", "1"))
        .await
        .expect("set quota-dlo metadata");

    let resp = handle_dlo_register(account, "quota-dlo", "dlo-1", "quota-copy-src/seg", &Some(creds.clone()))
        .await
        .expect("first DLO registration fits count=1");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let err = handle_dlo_register(account, "quota-dlo", "dlo-2", "quota-copy-src/seg", &Some(creds.clone()))
        .await
        .expect_err("second DLO registration must exceed count=1 quota");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
}

async fn scenario_copy_object_destination_quota(account: &str, creds: &Credentials) {
    update_container_metadata(account, "quota-copy-dst", creds, MetadataUpdate::default().set("quota-bytes", "500"))
        .await
        .expect("set quota-copy-dst metadata");

    put_object(
        account,
        "quota-copy-src",
        "big.bin",
        creds,
        Cursor::new(vec![0x11u8; 600]),
        &headers_with_len(600),
    )
    .await
    .expect("put big source");
    put_object(
        account,
        "quota-copy-src",
        "small.bin",
        creds,
        Cursor::new(vec![0x22u8; 200]),
        &headers_with_len(200),
    )
    .await
    .expect("put small source");

    let err = copy_object(
        account,
        "quota-copy-src",
        "big.bin",
        account,
        "quota-copy-dst",
        "copied-big.bin",
        creds,
        &HeaderMap::new(),
    )
    .await
    .expect_err("COPY exceeding destination quota must fail");
    assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
    assert!(matches!(
        head_object(account, "quota-copy-dst", "copied-big.bin", creds).await,
        Err(SwiftError::NotFound(_))
    ));

    copy_object(
        account,
        "quota-copy-src",
        "small.bin",
        account,
        "quota-copy-dst",
        "copied-small.bin",
        creds,
        &HeaderMap::new(),
    )
    .await
    .expect("COPY within destination quota must succeed");
    assert!(
        head_object(account, "quota-copy-dst", "copied-small.bin", creds)
            .await
            .is_ok()
    );

    // Same-object self-copy (metadata update in place) must complete without deadlocking
    // on ECStore object locks and must preserve exact payload bytes.
    let mut self_copy_headers = HeaderMap::new();
    self_copy_headers.insert("x-object-meta-revision", "2".parse().expect("valid header"));
    copy_object(
        account,
        "quota-copy-dst",
        "copied-small.bin",
        account,
        "quota-copy-dst",
        "copied-small.bin",
        creds,
        &self_copy_headers,
    )
    .await
    .expect("same-object self-copy must succeed without deadlocking");
    let updated_info = head_object(account, "quota-copy-dst", "copied-small.bin", creds)
        .await
        .expect("HEAD after self-copy");
    assert_eq!(updated_info.user_defined.get("revision").map(String::as_str), Some("2"));
    let mut reader = get_object(account, "quota-copy-dst", "copied-small.bin", creds, None)
        .await
        .expect("GET after self-copy");
    let mut read_back = Vec::new();
    reader.stream.read_to_end(&mut read_back).await.expect("read after self-copy");
    assert_eq!(read_back, vec![0x22u8; 200]);

    // Credential-less DLO registration and put_object_with_metadata must fail closed
    // with 401 Unauthorized rather than bypassing container quotas.
    let err = handle_dlo_register(account, "quota-dlo", "unauth-dlo", "quota-copy-src/seg", &None)
        .await
        .expect_err("credential-less DLO registration must be rejected");
    assert!(matches!(err, SwiftError::Unauthorized(_)));

    let err = put_object_with_metadata(
        account,
        "quota-put",
        "unauth-meta.bin",
        &None,
        Cursor::new(b"blocked".to_vec()),
        &HashMap::new(),
    )
    .await
    .expect_err("credential-less put_object_with_metadata must be rejected");
    assert!(matches!(err, SwiftError::Unauthorized(_)));
}
