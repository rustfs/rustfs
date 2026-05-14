#![cfg(feature = "rio-v2")]

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use rustfs_ecstore::bitrot::create_bitrot_reader;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::disk::{DiskAPI as _, DiskOption, new_disk};
use rustfs_ecstore::store_api::{GetObjectReader, ObjectInfo, ObjectOptions};
use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use temp_env::async_with_vars;
use tokio::io::AsyncReadExt;

#[derive(Debug, Deserialize)]
struct ManifestRecord {
    bucket: String,
    object: String,
    backend_files: Vec<String>,
}

fn fixture_root() -> PathBuf {
    std::env::var_os("RUSTFS_MINIO_FIXTURE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rio-v2/tests/fixtures/minio-generated"))
}

fn case_dir(case_id: &str) -> PathBuf {
    fixture_root().join("cases").join(case_id)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let text = fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

fn require_fixture_case(case_id: &str) -> PathBuf {
    let path = case_dir(case_id);
    assert!(
        path.is_dir(),
        "fixture case missing: {}. Run scripts/minio_fixture_lab/lab.py capture-matrix first.",
        path.display()
    );
    path
}

fn read_plaintext_sha256(case_dir: &Path) -> String {
    fs::read_to_string(case_dir.join("plaintext.sha256"))
        .unwrap_or_else(|err| panic!("read plaintext.sha256 under {}: {err}", case_dir.display()))
        .trim()
        .to_string()
}

fn minio_static_kms_key_b64() -> String {
    std::env::var("RUSTFS_MINIO_STATIC_KMS_KEY_B64")
        .unwrap_or_else(|_| panic!("RUSTFS_MINIO_STATIC_KMS_KEY_B64 must point to the 32-byte static MinIO KMS key"))
}

fn object_xl_meta_path(case_dir: &Path, manifest: &ManifestRecord) -> PathBuf {
    let expected = format!("disk1/{}/{}/xl.meta", manifest.bucket, manifest.object);
    let relative = manifest
        .backend_files
        .iter()
        .find(|entry| entry.as_str() == expected)
        .unwrap_or_else(|| panic!("object xl.meta missing from manifest backend_files: {expected}"));
    case_dir.join("backend").join(relative)
}

fn load_file_info(case_dir: &Path, manifest: &ManifestRecord) -> FileInfo {
    let xl_meta_path = object_xl_meta_path(case_dir, manifest);
    let xl_meta = fs::read(&xl_meta_path).unwrap_or_else(|err| panic!("read {}: {err}", xl_meta_path.display()));
    get_file_info(
        &xl_meta,
        &manifest.bucket,
        &manifest.object,
        "",
        FileInfoOpts {
            data: true,
            include_free_versions: true,
        },
    )
    .unwrap_or_else(|err| panic!("decode {}: {err}", xl_meta_path.display()))
}

fn load_object_info(file_info: &FileInfo, manifest: &ManifestRecord) -> ObjectInfo {
    ObjectInfo::from_file_info(file_info, &manifest.bucket, &manifest.object, false)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

async fn encrypted_fixture_bytes(case_dir: &Path, manifest: &ManifestRecord, file_info: &FileInfo) -> Vec<u8> {
    let disk_root = case_dir.join("backend").join("disk1");
    let disk_root_str = disk_root
        .to_str()
        .unwrap_or_else(|| panic!("non-utf8 disk root {}", disk_root.display()));
    let mut endpoint = Endpoint::try_from(disk_root_str).expect("fixture disk endpoint");
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("open fixture disk");
    let data_dir = file_info
        .data_dir
        .as_ref()
        .unwrap_or_else(|| panic!("fixture {} is missing data_dir", manifest.object));

    let mut encrypted = Vec::new();
    for part in &file_info.parts {
        let checksum_info = file_info.erasure.get_checksum_info(part.number);
        let path = format!("{}/{}/part.{}", manifest.object, data_dir, part.number);
        let mut reader = create_bitrot_reader(
            None,
            Some(&disk),
            &manifest.bucket,
            &path,
            0,
            part.size as usize,
            file_info.erasure.shard_size(),
            checksum_info.algorithm.clone(),
            false,
            false,
        )
        .await
        .unwrap_or_else(|err| panic!("create bitrot reader for {path}: {err:?}"))
        .unwrap_or_else(|| panic!("missing bitrot reader for {path}"));
        let mut block = vec![0u8; file_info.erasure.shard_size()];
        loop {
            let n = reader
                .read(&mut block)
                .await
                .unwrap_or_else(|err| panic!("read decoded encrypted bytes from {path}: {err}"));
            if n == 0 {
                break;
            }
            encrypted.extend_from_slice(&block[..n]);
            if n < block.len() {
                break;
            }
        }
    }
    disk.close().await.expect("close fixture disk");
    encrypted
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_multipart_fixture() {
    assert_fixture_round_trip("sse-s3-multipart-8m", 8 * 1024 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_kms_multipart_fixture() {
    assert_fixture_round_trip("sse-kms-multipart-8m", 8 * 1024 * 1024).await;
}

async fn assert_fixture_round_trip(case_id: &str, expected_size: i64) {
    let case_dir = require_fixture_case(case_id);
    let manifest: ManifestRecord = read_json(&case_dir.join("manifest.json"));
    let expected_sha256 = read_plaintext_sha256(&case_dir);
    let file_info = load_file_info(&case_dir, &manifest);
    let encrypted = encrypted_fixture_bytes(&case_dir, &manifest, &file_info).await;
    let object_info = load_object_info(&file_info, &manifest);
    let kms_key_b64 = minio_static_kms_key_b64();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", Some(kms_key_b64)),
            ("RUSTFS_SSE_S3_MASTER_KEY", None::<String>),
        ],
        async {
            let (mut reader, offset, length) = GetObjectReader::new(
                Box::new(Cursor::new(encrypted)),
                None,
                &object_info,
                &ObjectOptions::default(),
                &http::HeaderMap::new(),
            )
            .await
            .expect("construct GetObjectReader from MinIO raw fixture");

            let mut plaintext = Vec::new();
            reader
                .read_to_end(&mut plaintext)
                .await
                .expect("read plaintext from MinIO raw fixture");

            assert_eq!(offset, 0);
            assert_eq!(length, object_info.size);
            assert_eq!(reader.object_info.size, expected_size);
            assert_eq!(plaintext.len(), expected_size as usize);
            assert_eq!(sha256_hex(&plaintext), expected_sha256);
        },
    )
    .await;
}
