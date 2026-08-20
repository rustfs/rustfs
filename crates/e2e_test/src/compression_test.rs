//! Integration tests for object compression functionality

use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tracing::info;

const COMPRESSION_TEST_BUCKET: &str = "compression-test-bucket";
const MIN_COMPRESSIBLE_SIZE: usize = 4096;

fn generate_compressible_data(size: usize) -> Vec<u8> {
    let pattern = b"Hello, this is compressible test data! ";
    let mut data = Vec::with_capacity(size);
    let repetitions = size / pattern.len() + 1;
    for _ in 0..repetitions {
        data.extend_from_slice(pattern);
    }
    data.truncate(size);
    data
}

/// Deterministic 2048-byte-period binary pattern that compresses extremely well: every part
/// yields many compressed blocks, which is exactly the shape that reproduced the mid-payload
/// Pending truncation (rustfs/rustfs#5957).
fn generate_high_ratio_binary_data(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| ((i as u64).wrapping_mul(2_654_435_761).wrapping_add(seed as u64) >> 3) as u8)
        .collect()
}

fn find_part_files(temp_dir: &str, bucket: &str, object_key: &str) -> Vec<PathBuf> {
    let bucket_path = PathBuf::from(temp_dir).join(bucket);
    let mut part_files = Vec::new();

    fn scan_dir(dir: &PathBuf, target: &str, results: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_dir(&path, target, results);
                } else if path
                    .file_name()
                    .map(|n| n.to_string_lossy().starts_with("part."))
                    .unwrap_or(false)
                    && path.to_string_lossy().contains(target)
                {
                    results.push(path);
                }
            }
        }
    }

    scan_dir(&bucket_path, object_key, &mut part_files);
    part_files
}

async fn start_rustfs_with_compression(env: &mut RustFSTestEnvironment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env.cleanup_existing_processes().await?;

    let binary_path = rustfs_binary_path();
    // Route the child's stdout/stderr through the shared RUSTFS_E2E_LOG_DIR
    // capture (survives the temp-dir cleanup on Drop and is uploaded as a CI
    // artifact); without the env var the child inherits stdio as before.
    let mut command = Command::new(&binary_path);
    command
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_COMPRESSION_ENABLED", "true")
        .env("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true")
        .args([
            "--address",
            &env.address,
            "--access-key",
            &env.access_key,
            "--secret-key",
            &env.secret_key,
            &env.temp_dir,
        ]);
    crate::common::capture_command_logs(&mut command, env.capture_log_path.as_deref())?;
    let process = command.spawn()?;

    env.process = Some(process);

    info!("Waiting for RustFS server with compression enabled on {}", env.address);
    env.wait_for_server_ready().await
}

#[tokio::test]
async fn test_compression_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting compression roundtrip test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(COMPRESSION_TEST_BUCKET).await?;

    // Upload compressible object larger than MIN_COMPRESSIBLE_SIZE
    let original_size = MIN_COMPRESSIBLE_SIZE + 1024;
    let original_data = generate_compressible_data(original_size);
    let object_key = "test-compressible.txt";

    info!("Uploading {} bytes", original_size);
    client
        .put_object()
        .bucket(COMPRESSION_TEST_BUCKET)
        .key(object_key)
        .body(ByteStream::from(original_data.clone()))
        .send()
        .await?;

    // HEAD to verify size
    let head_response = client
        .head_object()
        .bucket(COMPRESSION_TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let content_length = head_response.content_length().unwrap_or(0);
    assert_eq!(content_length as usize, original_size, "Content-Length should be original size");

    let part_files = find_part_files(&env.temp_dir, COMPRESSION_TEST_BUCKET, object_key);
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();

    assert!(
        total_physical_size < original_size as u64,
        "Physical size {} should be less than original size {} (compression applied)",
        total_physical_size,
        original_size
    );
    info!(
        "Physical storage size: {} bytes (compressed from {} bytes)",
        total_physical_size, original_size
    );

    // GET and verify data
    let get_response = client
        .get_object()
        .bucket(COMPRESSION_TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();

    assert_eq!(downloaded_data.len(), original_size);
    assert_eq!(&downloaded_data[..], &original_data[..], "Data mismatch");

    info!("Compression roundtrip test passed");
    env.delete_test_bucket(COMPRESSION_TEST_BUCKET).await?;
    env.stop_server();
    Ok(())
}

const MULTIPART_COMPRESSION_BUCKET: &str = "compression-multipart-bucket";
const MPU_PART1_SIZE: usize = 5 * 1024 * 1024;
const MPU_PART2_SIZE: usize = 1024 * 1024;

async fn multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    parts: &[&[u8]],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

    let mut completed_parts = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let upload = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.to_vec()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(upload.e_tag().unwrap_or_default())
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    Ok(())
}

async fn fetch_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    range: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let response = client.get_object().bucket(bucket).key(key).range(range).send().await?;
    Ok(response.body.collect().await?.into_bytes().to_vec())
}

/// Multipart disk compression roundtrip: parts are written as independent
/// compressed streams and every GET shape must reassemble the original bytes
/// (rustfs/rustfs#5957: multipart uploads previously bypassed disk compression
/// entirely).
#[tokio::test]
async fn test_compression_multipart_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting multipart compression roundtrip test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(MULTIPART_COMPRESSION_BUCKET).await?;

    let object_key = "multipart-compressible.txt";
    let part1 = generate_compressible_data(MPU_PART1_SIZE);
    let part2 = generate_compressible_data(MPU_PART2_SIZE);
    let mut original_data = part1.clone();
    original_data.extend_from_slice(&part2);
    let total_size = original_data.len();

    multipart_upload(&client, MULTIPART_COMPRESSION_BUCKET, object_key, &[&part1, &part2]).await?;

    let head_response = client
        .head_object()
        .bucket(MULTIPART_COMPRESSION_BUCKET)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        head_response.content_length().unwrap_or(0) as usize,
        total_size,
        "Content-Length should be the logical object size"
    );

    let part_files = find_part_files(&env.temp_dir, MULTIPART_COMPRESSION_BUCKET, object_key);
    assert!(!part_files.is_empty(), "expected on-disk part files for the multipart object");
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    assert!(
        total_physical_size < (total_size / 2) as u64,
        "Physical size {total_physical_size} should be well below original size {total_size} (multipart compression applied)"
    );
    info!("Multipart physical storage size: {total_physical_size} bytes (compressed from {total_size} bytes)");

    // Full GET must reassemble both independently compressed parts.
    let get_response = client
        .get_object()
        .bucket(MULTIPART_COMPRESSION_BUCKET)
        .key(object_key)
        .send()
        .await?;
    let downloaded = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded.len(), total_size);
    assert_eq!(&downloaded[..], &original_data[..], "full GET data mismatch");

    // Range fully inside part 1.
    let range_inside_part1 = fetch_range(&client, MULTIPART_COMPRESSION_BUCKET, object_key, "bytes=1024-999423").await?;
    assert_eq!(&range_inside_part1[..], &original_data[1024..999424], "part-1 range mismatch");

    // Range crossing the part boundary.
    let boundary_start = MPU_PART1_SIZE - 128 * 1024;
    let boundary_end = MPU_PART1_SIZE + 128 * 1024 - 1;
    let range_crossing = fetch_range(
        &client,
        MULTIPART_COMPRESSION_BUCKET,
        object_key,
        &format!("bytes={boundary_start}-{boundary_end}"),
    )
    .await?;
    assert_eq!(
        &range_crossing[..],
        &original_data[boundary_start..boundary_end + 1],
        "boundary-crossing range mismatch"
    );

    // Range fully inside part 2.
    let part2_start = MPU_PART1_SIZE + 4096;
    let part2_end = MPU_PART1_SIZE + 256 * 1024 - 1;
    let range_inside_part2 = fetch_range(
        &client,
        MULTIPART_COMPRESSION_BUCKET,
        object_key,
        &format!("bytes={part2_start}-{part2_end}"),
    )
    .await?;
    assert_eq!(
        &range_inside_part2[..],
        &original_data[part2_start..part2_end + 1],
        "part-2 range mismatch"
    );

    // Suffix range (last 128 KiB, entirely in part 2).
    let suffix_len = 128 * 1024;
    let suffix = fetch_range(&client, MULTIPART_COMPRESSION_BUCKET, object_key, &format!("bytes=-{suffix_len}")).await?;
    assert_eq!(&suffix[..], &original_data[total_size - suffix_len..], "suffix range mismatch");

    // partNumber GETs must return each original part.
    for (part_number, expected) in [(1, &part1), (2, &part2)] {
        let response = client
            .get_object()
            .bucket(MULTIPART_COMPRESSION_BUCKET)
            .key(object_key)
            .part_number(part_number)
            .send()
            .await?;
        let body = response.body.collect().await?.into_bytes();
        assert_eq!(&body[..], &expected[..], "partNumber={part_number} GET mismatch");
    }

    info!("Multipart compression roundtrip test passed");
    env.delete_test_bucket(MULTIPART_COMPRESSION_BUCKET).await?;
    env.stop_server();
    Ok(())
}

const MPU_HIGH_RATIO_BUCKET: &str = "compression-mpu-high-ratio-bucket";

/// High-ratio binary multipart payload: the object key is on the compression allow-list, so the
/// disk-compression path runs and each part is stored as many compressed blocks — the shape that
/// reproduced the mid-payload Pending truncation (rustfs/rustfs#5957). Every GET shape must return
/// the exact original bytes, and the stored size must show the data really was compressed.
#[tokio::test]
async fn test_compression_multipart_high_ratio_binary_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting multipart high-ratio binary compression roundtrip test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(MPU_HIGH_RATIO_BUCKET).await?;

    let object_key = "multipart-high-ratio.txt";
    let part1 = generate_high_ratio_binary_data(MPU_PART1_SIZE, 7);
    let part2 = generate_high_ratio_binary_data(MPU_PART2_SIZE, 61);
    let mut original_data = part1.clone();
    original_data.extend_from_slice(&part2);
    let total_size = original_data.len();

    multipart_upload(&client, MPU_HIGH_RATIO_BUCKET, object_key, &[&part1, &part2]).await?;

    let head_response = client
        .head_object()
        .bucket(MPU_HIGH_RATIO_BUCKET)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        head_response.content_length().unwrap_or(0) as usize,
        total_size,
        "Content-Length should be the logical object size"
    );

    // This pattern compresses to roughly 1/50 of its logical size, so a comfortably loose 2x
    // margin still proves the parts were stored compressed rather than raw or double-encoded.
    let part_files = find_part_files(&env.temp_dir, MPU_HIGH_RATIO_BUCKET, object_key);
    assert!(!part_files.is_empty(), "expected on-disk part files for the multipart object");
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    assert!(
        total_physical_size < (total_size as u64) / 2,
        "Physical size {total_physical_size} should be far below the logical size {total_size} for high-ratio data"
    );
    info!("High-ratio multipart physical storage size: {total_physical_size} bytes (logical {total_size} bytes)");

    info!("step: full GET");
    let get_response = client
        .get_object()
        .bucket(MPU_HIGH_RATIO_BUCKET)
        .key(object_key)
        .send()
        .await?;
    let downloaded = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded.len(), total_size);
    assert_eq!(&downloaded[..], &original_data[..], "full GET data mismatch");

    // Range crossing the part boundary.
    info!("step: boundary range GET");
    let boundary_start = MPU_PART1_SIZE - 128 * 1024;
    let boundary_end = MPU_PART1_SIZE + 128 * 1024 - 1;
    let range_crossing = fetch_range(
        &client,
        MPU_HIGH_RATIO_BUCKET,
        object_key,
        &format!("bytes={boundary_start}-{boundary_end}"),
    )
    .await?;
    assert_eq!(
        &range_crossing[..],
        &original_data[boundary_start..boundary_end + 1],
        "boundary-crossing range mismatch"
    );

    // partNumber GET for the trailing part.
    info!("step: partNumber GET");
    let part2_response = client
        .get_object()
        .bucket(MPU_HIGH_RATIO_BUCKET)
        .key(object_key)
        .part_number(2)
        .send()
        .await?;
    let part2_body = part2_response.body.collect().await?.into_bytes();
    assert_eq!(&part2_body[..], &part2[..], "partNumber=2 GET mismatch");

    info!("Multipart high-ratio binary compression roundtrip test passed");
    env.delete_test_bucket(MPU_HIGH_RATIO_BUCKET).await?;
    env.stop_server();
    Ok(())
}

const MPU_COPY_COMPRESSION_BUCKET: &str = "compression-mpu-copy-bucket";
const MPU_COPY_SOURCE_SIZE: usize = 6 * 1024 * 1024;
const MPU_COPY_RANGE_LEN: usize = 5 * 1024 * 1024;

/// UploadPartCopy feeds a part from an already stored (and already compressed) object. The copied
/// range must be decompressed on read and re-compressed into the destination part, so the final
/// object has to match "source prefix + uploaded tail" byte for byte.
#[tokio::test]
async fn test_compression_multipart_upload_part_copy_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting multipart upload-part-copy compression roundtrip test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(MPU_COPY_COMPRESSION_BUCKET).await?;

    // Source object: a plain PUT that goes through the single-stream compression path.
    let source_key = "copy-source.txt";
    let source_data = generate_compressible_data(MPU_COPY_SOURCE_SIZE);
    client
        .put_object()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(source_key)
        .body(ByteStream::from(source_data.clone()))
        .send()
        .await?;

    // Destination object: part 1 copied from the source, part 2 uploaded directly.
    let target_key = "copy-target.txt";
    let part2 = generate_compressible_data(MPU_PART2_SIZE);
    let mut expected_data = source_data[..MPU_COPY_RANGE_LEN].to_vec();
    expected_data.extend_from_slice(&part2);
    let total_size = expected_data.len();

    let create = client
        .create_multipart_upload()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .send()
        .await?;
    let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

    let copy_part = client
        .upload_part_copy()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{MPU_COPY_COMPRESSION_BUCKET}/{source_key}"))
        .copy_source_range(format!("bytes=0-{}", MPU_COPY_RANGE_LEN - 1))
        .send()
        .await?;
    let copy_etag = copy_part
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .ok_or("missing copy part etag")?
        .to_string();

    let uploaded_part = client
        .upload_part()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await?;

    client
        .complete_multipart_upload()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1).e_tag(copy_etag).build())
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(uploaded_part.e_tag().unwrap_or_default())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let head_response = client
        .head_object()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .send()
        .await?;
    assert_eq!(
        head_response.content_length().unwrap_or(0) as usize,
        total_size,
        "Content-Length should be the logical object size"
    );

    let part_files = find_part_files(&env.temp_dir, MPU_COPY_COMPRESSION_BUCKET, target_key);
    assert!(!part_files.is_empty(), "expected on-disk part files for the copied object");
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    assert!(
        total_physical_size < (total_size / 2) as u64,
        "Physical size {total_physical_size} should be well below original size {total_size} (copied part compression applied)"
    );

    let get_response = client
        .get_object()
        .bucket(MPU_COPY_COMPRESSION_BUCKET)
        .key(target_key)
        .send()
        .await?;
    let downloaded = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded.len(), total_size);
    assert_eq!(&downloaded[..], &expected_data[..], "copied multipart GET data mismatch");

    info!("Multipart upload-part-copy compression roundtrip test passed");
    env.delete_test_bucket(MPU_COPY_COMPRESSION_BUCKET).await?;
    env.stop_server();
    Ok(())
}

const MPU_THREE_PARTS_BUCKET: &str = "compression-mpu-three-parts-bucket";
const MPU_THREE_PARTS_TAIL_SIZE: usize = 512 * 1024;

/// Three-part upload with uneven part sizes: each partNumber GET must map back to exactly one
/// compressed part stream, and a suffix range must resolve inside the trailing part.
#[tokio::test]
async fn test_compression_multipart_three_parts_part_number_gets() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting three-part multipart compression partNumber test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(MPU_THREE_PARTS_BUCKET).await?;

    let object_key = "multipart-three-parts.txt";
    let part1 = generate_compressible_data(MPU_PART1_SIZE);
    let part2 = generate_compressible_data(MPU_PART1_SIZE);
    let part3 = generate_compressible_data(MPU_THREE_PARTS_TAIL_SIZE);
    let mut original_data = part1.clone();
    original_data.extend_from_slice(&part2);
    original_data.extend_from_slice(&part3);
    let total_size = original_data.len();

    multipart_upload(&client, MPU_THREE_PARTS_BUCKET, object_key, &[&part1, &part2, &part3]).await?;

    let head_response = client
        .head_object()
        .bucket(MPU_THREE_PARTS_BUCKET)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        head_response.content_length().unwrap_or(0) as usize,
        total_size,
        "Content-Length should be the logical object size"
    );

    let part_files = find_part_files(&env.temp_dir, MPU_THREE_PARTS_BUCKET, object_key);
    assert!(!part_files.is_empty(), "expected on-disk part files for the multipart object");
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    assert!(
        total_physical_size < (total_size / 2) as u64,
        "Physical size {total_physical_size} should be well below original size {total_size} (multipart compression applied)"
    );

    // Every partNumber GET must return exactly the bytes of the corresponding uploaded part.
    for (part_number, expected) in [(1, &part1), (2, &part2), (3, &part3)] {
        let response = client
            .get_object()
            .bucket(MPU_THREE_PARTS_BUCKET)
            .key(object_key)
            .part_number(part_number)
            .send()
            .await?;
        let body = response.body.collect().await?.into_bytes();
        assert_eq!(&body[..], &expected[..], "partNumber={part_number} GET mismatch");
    }

    // Suffix range (last 64 KiB) resolves inside the trailing part.
    let suffix_len = 64 * 1024;
    let suffix = fetch_range(&client, MPU_THREE_PARTS_BUCKET, object_key, &format!("bytes=-{suffix_len}")).await?;
    assert_eq!(&suffix[..], &original_data[total_size - suffix_len..], "suffix range mismatch");

    info!("Three-part multipart compression partNumber test passed");
    env.delete_test_bucket(MPU_THREE_PARTS_BUCKET).await?;
    env.stop_server();
    Ok(())
}

const MPU_SSE_COMPRESSION_BUCKET: &str = "compression-mpu-sse-bucket";

async fn start_rustfs_with_compression_and_sse(
    env: &mut RustFSTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use base64::Engine;
    env.cleanup_existing_processes().await?;

    let binary_path = rustfs_binary_path();
    let master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    // Server output goes to a file inside the per-test temp dir so a failing
    // run can be diagnosed from the child's logs.
    let server_log = std::fs::File::create(format!("{}/server.log", env.temp_dir))?;
    let server_log_err = server_log.try_clone()?;
    let process = Command::new(&binary_path)
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_COMPRESSION_ENABLED", "true")
        .env("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true")
        .env("RUSTFS_SSE_S3_MASTER_KEY", master_key)
        .env("RUST_LOG", "rustfs=info,rustfs_ecstore=info")
        .stdout(std::process::Stdio::from(server_log))
        .stderr(std::process::Stdio::from(server_log_err))
        .args([
            "--address",
            &env.address,
            "--access-key",
            &env.access_key,
            "--secret-key",
            &env.secret_key,
            &env.temp_dir,
        ])
        .spawn()?;

    env.process = Some(process);

    info!("Waiting for RustFS server with compression + SSE-S3 enabled on {}", env.address);
    env.wait_for_server_ready().await
}

/// SSE-S3 + disk compression multipart: each part is compressed and then encrypted, and every GET
/// shape must still return the original plaintext bytes. Physical size must shrink because the
/// compression runs before encryption.
#[tokio::test]
async fn test_compression_multipart_sse_s3_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use aws_sdk_s3::types::ServerSideEncryption;

    init_logging();
    info!("Starting SSE-S3 multipart compression roundtrip test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression_and_sse(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(MPU_SSE_COMPRESSION_BUCKET).await?;

    let object_key = "multipart-sse-compressible.txt";
    let part1 = generate_compressible_data(MPU_PART1_SIZE);
    let part2 = generate_compressible_data(MPU_PART2_SIZE);
    let mut original_data = part1.clone();
    original_data.extend_from_slice(&part2);
    let total_size = original_data.len();

    let create = client
        .create_multipart_upload()
        .bucket(MPU_SSE_COMPRESSION_BUCKET)
        .key(object_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

    let mut completed_parts = Vec::new();
    for (i, part) in [&part1, &part2].into_iter().enumerate() {
        let part_number = (i + 1) as i32;
        let upload = client
            .upload_part()
            .bucket(MPU_SSE_COMPRESSION_BUCKET)
            .key(object_key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(upload.e_tag().unwrap_or_default())
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(MPU_SSE_COMPRESSION_BUCKET)
        .key(object_key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;

    let head_response = client
        .head_object()
        .bucket(MPU_SSE_COMPRESSION_BUCKET)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        head_response.content_length().unwrap_or(0) as usize,
        total_size,
        "Content-Length should be the logical object size"
    );
    assert_eq!(
        head_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HEAD must report SSE-S3"
    );

    let part_files = find_part_files(&env.temp_dir, MPU_SSE_COMPRESSION_BUCKET, object_key);
    assert!(!part_files.is_empty(), "expected on-disk part files for the multipart object");
    let total_physical_size: u64 = part_files.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    assert!(
        total_physical_size < (total_size / 2) as u64,
        "Physical size {total_physical_size} should be well below original size {total_size} (compress-then-encrypt applied)"
    );

    let get_response = client
        .get_object()
        .bucket(MPU_SSE_COMPRESSION_BUCKET)
        .key(object_key)
        .send()
        .await?;
    let downloaded = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded.len(), total_size);
    assert_eq!(&downloaded[..], &original_data[..], "SSE-S3 multipart full GET data mismatch");

    // Range crossing the part boundary must decrypt and decompress across parts.
    let boundary_start = MPU_PART1_SIZE - 64 * 1024;
    let boundary_end = MPU_PART1_SIZE + 64 * 1024 - 1;
    let range_crossing = fetch_range(
        &client,
        MPU_SSE_COMPRESSION_BUCKET,
        object_key,
        &format!("bytes={boundary_start}-{boundary_end}"),
    )
    .await?;
    assert_eq!(
        &range_crossing[..],
        &original_data[boundary_start..boundary_end + 1],
        "SSE-S3 boundary-crossing range mismatch"
    );

    // partNumber GET for the trailing part.
    let part2_response = client
        .get_object()
        .bucket(MPU_SSE_COMPRESSION_BUCKET)
        .key(object_key)
        .part_number(2)
        .send()
        .await?;
    let part2_body = part2_response.body.collect().await?.into_bytes();
    assert_eq!(&part2_body[..], &part2[..], "SSE-S3 partNumber=2 GET mismatch");

    info!("SSE-S3 multipart compression roundtrip test passed");
    env.delete_test_bucket(MPU_SSE_COMPRESSION_BUCKET).await?;
    env.stop_server();
    Ok(())
}
