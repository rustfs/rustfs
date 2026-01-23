//! Integration tests for object compression functionality

use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
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
    let process = Command::new(&binary_path)
        .env("RUSTFS_COMPRESSION_ENABLED", "true")
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

    info!("Waiting for RustFS server with compression enabled on {}", env.address);
    for i in 0..30 {
        if TcpStream::connect(&env.address).await.is_ok() {
            info!("RustFS server is ready after {} attempts", i + 1);
            return Ok(());
        }
        if i == 29 {
            return Err("RustFS server failed to become ready".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

#[tokio::test]
#[serial]
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
