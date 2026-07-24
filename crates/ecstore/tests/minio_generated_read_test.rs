#![cfg(feature = "rio-v2")]

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

mod storage_api;

use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use rustfs_utils::HashAlgorithm;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use storage_api::minio_generated_read::{
    DiskAPI as _, DiskOption, Endpoint, Erasure, GetObjectReader, HTTPRangeSpec, ObjectInfo, ObjectOptions, create_bitrot_reader,
    new_disk,
};
use temp_env::async_with_vars;
use tokio::io::{AsyncReadExt, AsyncWrite};

#[derive(Debug, Deserialize)]
struct ManifestRecord {
    bucket: String,
    object: String,
    backend_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RequestRecord {
    headers: std::collections::HashMap<String, String>,
}

#[derive(Default)]
struct VecAsyncWriter {
    bytes: Vec<u8>,
}

impl AsyncWrite for VecAsyncWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.bytes.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn fixture_root() -> PathBuf {
    std::env::var_os("RUSTFS_MINIO_FIXTURE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rio-v2/tests/fixtures/minio-generated"))
}

fn case_dir(case_id: &str) -> PathBuf {
    fixture_root().join("cases").join(case_id)
}

fn beta5_fixture_root() -> PathBuf {
    std::env::var_os("RUSTFS_BETA5_FIXTURE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rio-v2/tests/fixtures/rustfs-beta5-generated"))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let text = fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

fn read_plaintext_sha256(case_dir: &Path) -> String {
    fs::read_to_string(case_dir.join("plaintext.sha256"))
        .unwrap_or_else(|err| panic!("read plaintext.sha256 under {}: {err}", case_dir.display()))
        .trim()
        .to_string()
}

fn minio_static_kms_key() -> String {
    std::env::var("RUSTFS_MINIO_STATIC_KMS_KEY")
        .unwrap_or_else(|_| panic!("RUSTFS_MINIO_STATIC_KMS_KEY must use <key-id>:<base64-32-byte-key>"))
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

async fn load_fixture_reader_input(case_id: &str) -> (ObjectInfo, Vec<u8>, String, http::HeaderMap) {
    load_fixture_reader_input_from(case_dir(case_id)).await
}

async fn load_fixture_reader_input_from(case_dir: PathBuf) -> (ObjectInfo, Vec<u8>, String, http::HeaderMap) {
    assert!(case_dir.is_dir(), "fixture case missing: {}", case_dir.display());
    let manifest: ManifestRecord = read_json(&case_dir.join("manifest.json"));
    let request: RequestRecord = read_json(&case_dir.join("request.json"));
    let expected_sha256 = read_plaintext_sha256(&case_dir);
    let file_info = load_file_info(&case_dir, &manifest);
    let encrypted = encrypted_fixture_bytes(&case_dir, &manifest, &file_info).await;
    let object_info = load_object_info(&file_info, &manifest);

    let mut headers = http::HeaderMap::new();
    for (name, value) in request.headers {
        let name = http::header::HeaderName::from_bytes(name.as_bytes())
            .unwrap_or_else(|err| panic!("invalid fixture request header {name}: {err}"));
        let value =
            http::HeaderValue::try_from(value).unwrap_or_else(|err| panic!("invalid fixture request header value: {err}"));
        headers.insert(name, value);
    }

    (object_info, encrypted, expected_sha256, headers)
}

async fn read_fixture_plaintext(
    encrypted: Vec<u8>,
    object_info: ObjectInfo,
    headers: http::HeaderMap,
    static_kms_key: Option<String>,
    range: Option<HTTPRangeSpec>,
) -> Result<Vec<u8>, String> {
    let object_size = object_info.size;
    let full_object = range.is_none();

    async_with_vars(
        [
            ("RUSTFS_MINIO_STATIC_KMS_KEY", static_kms_key),
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
        ],
        async move {
            let (mut reader, offset, length) =
                GetObjectReader::new(Box::new(Cursor::new(encrypted)), range, &object_info, &ObjectOptions::default(), &headers)
                    .await
                    .map_err(|err| format!("construct GetObjectReader from MinIO raw fixture: {err:?}"))?;

            if full_object && (offset != 0 || length != object_size) {
                return Err(format!("unexpected fixture range offset={offset} length={length} size={object_size}"));
            }

            let mut plaintext = Vec::new();
            reader
                .read_to_end(&mut plaintext)
                .await
                .map_err(|err| format!("read plaintext from MinIO raw fixture: {err}"))?;

            Ok(plaintext)
        },
    )
    .await
}

async fn encrypted_fixture_bytes(case_dir: &Path, manifest: &ManifestRecord, file_info: &FileInfo) -> Vec<u8> {
    let erasure = Erasure::new_with_options(
        file_info.erasure.data_blocks,
        file_info.erasure.parity_blocks,
        file_info.erasure.block_size,
        file_info.uses_legacy_checksum,
    );
    let mut disks = Vec::with_capacity(file_info.erasure.distribution.len());
    for disk_number in 1..=file_info.erasure.distribution.len() {
        let disk_root = case_dir.join("backend").join(format!("disk{disk_number}"));
        let disk_root_str = disk_root
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 disk root {}", disk_root.display()));
        let mut endpoint = Endpoint::try_from(disk_root_str).expect("fixture disk endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_number - 1);
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .unwrap_or_else(|err| panic!("open fixture disk {disk_number}: {err}"));
        disks.push(disk);
    }
    let mut disk_order = vec![None; disks.len()];
    for (idx, disk) in disks.iter().enumerate() {
        let block_index = file_info.erasure.distribution[idx];
        disk_order[block_index - 1] = Some(disk);
    }
    let data_dir = file_info
        .data_dir
        .as_ref()
        .unwrap_or_else(|| panic!("fixture {} is missing data_dir", manifest.object));

    let mut encrypted = Vec::new();
    for part in &file_info.parts {
        let checksum_info = file_info.erasure.get_checksum_info(part.number);
        let checksum_algorithm = if file_info.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum_info.algorithm.clone()
        };
        let path = format!("{}/{}/part.{}", manifest.object, data_dir, part.number);
        let shard_read_len = erasure.shard_file_size(part.size as i64);
        let mut readers = Vec::with_capacity(disks.len());
        for (idx, disk) in disk_order.iter().enumerate() {
            let reader = create_bitrot_reader(
                None,
                *disk,
                &manifest.bucket,
                &path,
                0,
                shard_read_len as usize,
                file_info.erasure.shard_size(),
                checksum_algorithm.clone(),
                false,
                false,
            )
            .await
            .unwrap_or_else(|err| panic!("create bitrot reader for disk{} {path}: {err:?}", idx + 1));
            readers.push(reader);
        }

        let mut writer = VecAsyncWriter::default();
        let (written, err) = erasure.decode(&mut writer, readers, 0, part.size, part.size).await;
        if let Some(err) = err {
            panic!("decode erasure shards for {path}: {err}");
        }
        assert_eq!(written, part.size, "decoded part size should match xl.meta part size");
        encrypted.extend_from_slice(&writer.bytes);
    }
    for disk in disks {
        disk.close().await.expect("close fixture disk");
    }
    encrypted
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_multipart_fixture() {
    assert_fixture_round_trip("sse-s3-multipart-8m", 8 * 1024 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_singlepart_fixture() {
    assert_fixture_round_trip("sse-s3-singlepart-64k", 64 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_kms_multipart_fixture() {
    assert_fixture_round_trip("sse-kms-multipart-8m", 8 * 1024 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_kms_singlepart_fixture() {
    assert_fixture_round_trip("sse-kms-singlepart-64k", 64 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_c_multipart_fixture() {
    assert_fixture_round_trip("sse-c-multipart-8m", 8 * 1024 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_c_singlepart_fixture() {
    assert_fixture_round_trip("sse-c-singlepart-64k", 64 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_range_fixture() {
    assert_fixture_range_round_trip("sse-s3-multipart-8m").await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_kms_range_fixture() {
    assert_fixture_range_round_trip("sse-kms-multipart-8m").await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_c_range_fixture() {
    assert_fixture_range_round_trip("sse-c-multipart-8m").await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn rejects_minio_generated_sse_s3_fixture_with_wrong_kms_key() {
    let (object_info, encrypted, _, headers) = load_fixture_reader_input("sse-s3-multipart-8m").await;
    let wrong_key_b64 = "minio-default-key:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string();

    let result = read_fixture_plaintext(encrypted, object_info, headers, Some(wrong_key_b64), None).await;

    assert!(result.is_err(), "wrong KMS key must fail closed");
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn rejects_minio_generated_sse_s3_fixture_with_truncated_ciphertext() {
    let (object_info, mut encrypted, _, headers) = load_fixture_reader_input("sse-s3-multipart-8m").await;
    encrypted.truncate(encrypted.len() / 2);

    let result = read_fixture_plaintext(encrypted, object_info, headers, Some(minio_static_kms_key()), None).await;

    assert!(result.is_err(), "truncated ciphertext must return an explicit read error");
}

async fn assert_fixture_round_trip(case_id: &str, expected_size: i64) {
    let (object_info, encrypted, expected_sha256, headers) = load_fixture_reader_input(case_id).await;
    // `ObjectInfo.size` is the on-disk size. For SSE objects that is the
    // DARE-encrypted size (plaintext + 32 bytes per 64 KiB block), which is
    // deliberately larger than the logical object size. The size a client sees
    // (and what MinIO records via `x-*-internal-actual-size`) comes from
    // `decrypted_size()`/`get_actual_size()`, so assert against that — the raw
    // `size` field would never equal the plaintext length for encrypted objects.
    let decrypted_size = object_info.decrypted_size().expect("decrypted size from MinIO metadata");
    let kms_key = minio_static_kms_key();

    let plaintext = read_fixture_plaintext(encrypted, object_info, headers, Some(kms_key), None)
        .await
        .expect("fixture must restore with the configured KMS key");

    assert_eq!(decrypted_size, expected_size);
    assert_eq!(plaintext.len(), expected_size as usize);
    assert_eq!(sha256_hex(&plaintext), expected_sha256);
}

async fn assert_fixture_range_round_trip(case_id: &str) {
    const START: usize = 65_520;
    const END: usize = 65_680;

    let (object_info, encrypted, _, headers) = load_fixture_reader_input(case_id).await;
    let kms_key = minio_static_kms_key();
    let plaintext = read_fixture_plaintext(encrypted.clone(), object_info.clone(), headers.clone(), Some(kms_key.clone()), None)
        .await
        .expect("fixture full read must restore");
    let ranged = read_fixture_plaintext(
        encrypted,
        object_info,
        headers,
        Some(kms_key),
        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start: START as i64,
            end: END as i64,
        }),
    )
    .await
    .expect("fixture range read must restore");

    assert_eq!(ranged, plaintext[START..=END]);
}

#[tokio::test]
#[ignore = "requires a fixture generated by the pinned RustFS beta.5 release"]
async fn reads_real_rustfs_beta5_sse_kms_fixture_through_production_reader() {
    const CASE_ID: &str = "rustfs-beta5-sse-kms-singlepart-64k";
    const KMS_KEY_ID: &str = "beta5-test-key";

    let root = beta5_fixture_root();
    let manager = rustfs_kms::init_global_kms_service_manager();
    manager
        .configure(
            rustfs_kms::KmsConfig::local(root.join("kms"))
                .with_default_key(KMS_KEY_ID.to_string())
                .with_insecure_development_defaults(),
        )
        .await
        .expect("configure production local KMS for beta.5 fixture");
    manager.start().await.expect("start production local KMS for beta.5 fixture");

    let (object_info, encrypted, expected_sha256, headers) =
        load_fixture_reader_input_from(root.join("cases").join(CASE_ID)).await;
    let expected_size = object_info.decrypted_size().expect("beta.5 encrypted object size");
    let plaintext = read_fixture_plaintext(encrypted, object_info, headers, None, None)
        .await
        .expect("current production GET reader must decrypt the real beta.5 KMS fixture");

    assert_eq!(expected_size, 64 * 1024);
    assert_eq!(plaintext.len() as i64, expected_size);
    assert_eq!(sha256_hex(&plaintext), expected_sha256);
}
