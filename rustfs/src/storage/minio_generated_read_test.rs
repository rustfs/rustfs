#![cfg(feature = "rio-v2")]

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use super::sse::{SseObjectEncryptionResolver, reset_sse_dek_provider};
use super::storage_api::ecstore_test_support::{
    DiskAPI as _, DiskOption, Endpoint, Erasure, GetObjectReader, ObjectInfo, ObjectOptions, create_bitrot_reader, new_disk,
};
use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use temp_env::async_with_vars;
use tokio::io::{AsyncReadExt, AsyncWrite};

#[derive(Debug, Deserialize)]
struct ManifestRecord {
    bucket: String,
    object: String,
    backend_files: Vec<String>,
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
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../crates/rio-v2/tests/fixtures/minio-generated"))
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

/// Metadata as each disk stored it, indexed by disk position.
///
/// The object's primary `FileInfo` is read from one disk, but inline shard data
/// and bitrot checksums are per disk, so both have to come from the disk they
/// belong to.
fn load_per_disk_file_info(case_dir: &Path, manifest: &ManifestRecord, disk_count: usize) -> Vec<Option<FileInfo>> {
    (0..disk_count)
        .map(|idx| {
            let path = case_dir
                .join("backend")
                .join(format!("disk{}", idx + 1))
                .join(&manifest.bucket)
                .join(&manifest.object)
                .join("xl.meta");
            let bytes = fs::read(&path).ok()?;
            get_file_info(
                &bytes,
                &manifest.bucket,
                &manifest.object,
                "",
                FileInfoOpts {
                    data: true,
                    include_free_versions: true,
                    include_part_checksums: false,
                },
            )
            .ok()
        })
        .collect()
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
            include_part_checksums: false,
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

async fn load_fixture_reader_input(case_id: &str) -> (ObjectInfo, Vec<u8>, String) {
    let case_dir = require_fixture_case(case_id);
    let manifest: ManifestRecord = read_json(&case_dir.join("manifest.json"));
    let expected_sha256 = read_plaintext_sha256(&case_dir);
    let file_info = load_file_info(&case_dir, &manifest);
    let encrypted = encrypted_fixture_bytes(&case_dir, &manifest, &file_info).await;
    let object_info = load_object_info(&file_info, &manifest);

    (object_info, encrypted, expected_sha256)
}

async fn read_fixture_plaintext(encrypted: Vec<u8>, object_info: ObjectInfo, kms_key_b64: String) -> Result<Vec<u8>, String> {
    let object_size = object_info.size;

    // The DEK provider is cached process-wide once built, so without this reset
    // a case that ran earlier in the same binary keeps serving its master key to
    // every later case — which silently turned the wrong-key negative below into
    // a test that could not fail. Reset before each read so the provider is
    // built from the key this case actually configured.
    reset_sse_dek_provider();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", Some(kms_key_b64)),
            ("RUSTFS_SSE_S3_MASTER_KEY", None::<String>),
        ],
        async move {
            let resolver = SseObjectEncryptionResolver;
            let (mut reader, offset, length) = GetObjectReader::new_with_resolver(
                Box::new(Cursor::new(encrypted)),
                None,
                &object_info,
                &ObjectOptions::default(),
                &http::HeaderMap::new(),
                Some(&resolver),
            )
            .await
            .map_err(|err| format!("construct GetObjectReader from MinIO raw fixture: {err:?}"))?;

            if offset != 0 || length != object_size {
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
    // Pair each disk with the metadata that disk stored, then place both at the
    // erasure block slot that metadata claims — the shuffle the read path does
    // before it reads anything (`shuffle_disks_and_parts_metadata_by_index`).
    // The harness needs the per-disk metadata, not just the per-disk disk: below
    // the small-file threshold an object has no part file and each disk carries
    // its shard inline in its own xl.meta, and the bitrot checksums are per disk
    // too.
    let per_disk_meta = load_per_disk_file_info(case_dir, manifest, disks.len());
    let mut disk_order = vec![None; disks.len()];
    let mut meta_order: Vec<Option<FileInfo>> = vec![None; disks.len()];
    for (idx, disk) in disks.iter().enumerate() {
        let block_index = file_info.erasure.distribution[idx];
        disk_order[block_index - 1] = Some(disk);
        meta_order[block_index - 1] = per_disk_meta.get(idx).cloned().flatten();
    }
    let data_dir = file_info
        .data_dir
        .as_ref()
        .unwrap_or_else(|| panic!("fixture {} is missing data_dir", manifest.object));

    let mut encrypted = Vec::new();
    for part in &file_info.parts {
        let primary_checksum_info = file_info.erasure.get_checksum_info(part.number);
        let path = format!("{}/{}/part.{}", manifest.object, data_dir, part.number);
        let shard_read_len = file_info.erasure.shard_file_size(part.size as i64);
        let mut readers = Vec::with_capacity(disks.len());
        for (idx, disk) in disk_order.iter().enumerate() {
            // Below MinIO's small-file threshold there is no part file at all:
            // each disk keeps its erasure shard inline in its own xl.meta, with
            // the same bitrot framing the reader expects from a file.
            let disk_meta = meta_order[idx].as_ref();
            let inline_shard = disk_meta.and_then(|meta| meta.data.as_deref());
            let checksum_info = disk_meta
                .map(|meta| meta.erasure.get_checksum_info(part.number))
                .unwrap_or_else(|| primary_checksum_info.clone());
            let reader = create_bitrot_reader(
                inline_shard,
                *disk,
                &manifest.bucket,
                &path,
                0,
                shard_read_len as usize,
                file_info.erasure.shard_size(),
                checksum_info.algorithm.clone(),
                false,
                false,
            )
            .await
            .unwrap_or_else(|err| panic!("create bitrot reader for disk{} {path}: {err:?}", idx + 1));
            readers.push(reader);
        }

        let erasure = Erasure::try_new(
            file_info.erasure.data_blocks,
            file_info.erasure.parity_blocks,
            file_info.erasure.block_size,
        )
        .expect("fixture erasure geometry");
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
async fn reads_minio_generated_sse_kms_multipart_fixture() {
    assert_fixture_round_trip("sse-kms-multipart-8m", 8 * 1024 * 1024).await;
}

/// Read an SSE-C fixture, supplying the customer key the way a client does.
///
/// SSE-C needs no KMS at all — the key arrives on the request — so this path
/// shares nothing with the managed-SSE reads above beyond the fixture loader.
async fn read_ssec_fixture_plaintext(
    encrypted: Vec<u8>,
    object_info: ObjectInfo,
    customer_key_b64: &str,
    customer_key_md5_b64: &str,
) -> Result<Vec<u8>, String> {
    let object_size = object_info.size;
    reset_sse_dek_provider();

    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
        http::HeaderValue::from_static("AES256"),
    );
    headers.insert(
        http::HeaderName::from_static("x-amz-server-side-encryption-customer-key"),
        http::HeaderValue::from_str(customer_key_b64).expect("fixture customer key is a header value"),
    );
    headers.insert(
        http::HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5"),
        http::HeaderValue::from_str(customer_key_md5_b64).expect("fixture customer key md5 is a header value"),
    );

    let resolver = SseObjectEncryptionResolver;
    let (mut reader, offset, length) = GetObjectReader::new_with_resolver(
        Box::new(Cursor::new(encrypted)),
        None,
        &object_info,
        &ObjectOptions::default(),
        &headers,
        Some(&resolver),
    )
    .await
    .map_err(|err| format!("construct GetObjectReader from MinIO SSE-C fixture: {err:?}"))?;

    if offset != 0 || length != object_size {
        return Err(format!("unexpected fixture range offset={offset} length={length} size={object_size}"));
    }

    let mut plaintext = Vec::new();
    reader
        .read_to_end(&mut plaintext)
        .await
        .map_err(|err| format!("read plaintext from MinIO SSE-C fixture: {err}"))?;
    Ok(plaintext)
}

/// The interop claim must hold on the production key entry point, not only on
/// the test-only injection channel every other case here uses.
#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_fixture_through_production_master_key_env() {
    let (object_info, encrypted, expected_sha256) = load_fixture_reader_input("sse-s3-multipart-8m").await;

    let plaintext = read_fixture_plaintext_via_production_env(encrypted, object_info, minio_static_kms_key_b64())
        .await
        .expect("fixture must restore through RUSTFS_SSE_S3_MASTER_KEY");

    assert_eq!(sha256_hex(&plaintext), expected_sha256);
}

/// Objects small enough that MinIO inlined them into xl.meta instead of writing
/// a part file — the ordinary shape for everyday small objects, and the one
/// whose encrypted ETag misleads the multipart heuristic.
#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_s3_singlepart_fixture() {
    assert_fixture_round_trip("sse-s3-singlepart-64k", 64 * 1024).await;
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn reads_minio_generated_sse_kms_singlepart_fixture() {
    assert_fixture_round_trip("sse-kms-singlepart-64k", 64 * 1024).await;
}

/// SSE-C is the one managed shape needing no KMS: the customer supplies the key
/// on every request, so this measures the read path alone.
#[tokio::test]
#[ignore = "requires generated MinIO fixture data"]
async fn reads_minio_generated_sse_c_multipart_fixture() {
    // The fixture lab's fixed SSE-C key; recorded in the case's request.json.
    const SSEC_KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    const SSEC_KEY_MD5_B64: &str = "tP/LI3N87DFaSk0aoqYgzg==";

    let (object_info, encrypted, expected_sha256) = load_fixture_reader_input("sse-c-multipart-8m").await;

    let plaintext = read_ssec_fixture_plaintext(encrypted, object_info, SSEC_KEY_B64, SSEC_KEY_MD5_B64)
        .await
        .expect("MinIO SSE-C fixture must restore with the customer key");

    assert_eq!(sha256_hex(&plaintext), expected_sha256);
}

/// The read path skips the stored-MD5 comparison for MinIO SSE-C objects,
/// which store no MD5. This holds the line that made that safe: the customer
/// key is still proven by the object-key unseal, so a wrong key must fail even
/// with nothing to compare it against.
#[tokio::test]
#[ignore = "requires generated MinIO fixture data"]
async fn sse_c_wrong_customer_key_still_fails_without_a_stored_md5() {
    // A well-formed 32-byte key that is not the one the fixture was sealed
    // with, sent with its own correct MD5 so the request itself is valid.
    const WRONG_KEY_B64: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    const WRONG_KEY_MD5_B64: &str = "0YB4bMPPCf9SNlqiKmM0uQ==";

    let (object_info, encrypted, expected_sha256) = load_fixture_reader_input("sse-c-multipart-8m").await;

    let result = read_ssec_fixture_plaintext(encrypted, object_info, WRONG_KEY_B64, WRONG_KEY_MD5_B64).await;

    match result {
        Err(_) => {}
        // Never reached today, and asserted rather than assumed: if a future
        // change let a wrong key through, returning the real plaintext would be
        // the worst possible outcome.
        Ok(plaintext) => assert_ne!(
            sha256_hex(&plaintext),
            expected_sha256,
            "a wrong SSE-C customer key must never restore the original plaintext"
        ),
    }
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn rejects_minio_generated_sse_s3_fixture_with_wrong_kms_key() {
    let (object_info, encrypted, _) = load_fixture_reader_input("sse-s3-multipart-8m").await;
    let wrong_key_b64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string();

    let result = read_fixture_plaintext(encrypted, object_info, wrong_key_b64).await;

    assert!(result.is_err(), "wrong KMS key must fail closed");
}

#[tokio::test]
#[ignore = "requires generated MinIO fixture data and a local static KMS key"]
async fn rejects_minio_generated_sse_s3_fixture_with_truncated_ciphertext() {
    let (object_info, mut encrypted, expected_sha256) = load_fixture_reader_input("sse-s3-multipart-8m").await;
    encrypted.truncate(encrypted.len() / 2);

    let result = read_fixture_plaintext(encrypted, object_info, minio_static_kms_key_b64()).await;

    if let Ok(plaintext) = result {
        assert_ne!(
            sha256_hex(&plaintext),
            expected_sha256,
            "truncated ciphertext must not restore the original plaintext"
        );
    }
}

/// Read a fixture through the **production** provider selection.
///
/// [`read_fixture_plaintext`] injects the master key through
/// `__RUSTFS_SSE_SIMPLE_CMK`, which is `#[cfg(test)]`-only, so on its own it
/// proves nothing about a deployment: it never reaches
/// `LocalSseDekProvider::new_from_env`. This variant sets only
/// `RUSTFS_SSE_S3_MASTER_KEY` — the sole production entry point — so the
/// interop claim rests on the path operators actually run (backlog#1638).
async fn read_fixture_plaintext_via_production_env(
    encrypted: Vec<u8>,
    object_info: ObjectInfo,
    master_key_b64: String,
) -> Result<Vec<u8>, String> {
    let object_size = object_info.size;
    reset_sse_dek_provider();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(master_key_b64)),
        ],
        async move {
            let resolver = SseObjectEncryptionResolver;
            let (mut reader, offset, length) = GetObjectReader::new_with_resolver(
                Box::new(Cursor::new(encrypted)),
                None,
                &object_info,
                &ObjectOptions::default(),
                &http::HeaderMap::new(),
                Some(&resolver),
            )
            .await
            .map_err(|err| format!("construct GetObjectReader from MinIO raw fixture: {err:?}"))?;

            if offset != 0 || length != object_size {
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

async fn assert_fixture_round_trip(case_id: &str, expected_size: i64) {
    let (object_info, encrypted, expected_sha256) = load_fixture_reader_input(case_id).await;
    // `ObjectInfo.size` is the on-disk size. For SSE objects that is the
    // DARE-encrypted size (plaintext + 32 bytes per 64 KiB block), which is
    // deliberately larger than the logical object size. The size a client sees
    // (and what MinIO records via `x-*-internal-actual-size`) comes from
    // `decrypted_size()`/`get_actual_size()`, so assert against that — the raw
    // `size` field would never equal the plaintext length for encrypted objects.
    let decrypted_size = object_info.decrypted_size().expect("decrypted size from MinIO metadata");
    let kms_key_b64 = minio_static_kms_key_b64();

    let plaintext = read_fixture_plaintext(encrypted, object_info, kms_key_b64)
        .await
        .expect("fixture must restore with the configured KMS key");

    assert_eq!(decrypted_size, expected_size);
    assert_eq!(plaintext.len(), expected_size as usize);
    assert_eq!(sha256_hex(&plaintext), expected_sha256);
}
