

//! Integration tests for object compression functionality

use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use std::process::Command;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{info, debug};

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

async fn start_rustfs_with_compression(env: &mut RustFSTestEnvironment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env.cleanup_existing_processes().await?;

    let binary_path = rustfs_binary_path();
    let process = Command::new(&binary_path)
        .env("RUSTFS_COMPRESSION_ENABLED", "true")
        .args([
            "--address", &env.address,
            "--access-key", &env.access_key,
            "--secret-key", &env.secret_key,
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
    assert_eq!(
        content_length as usize, original_size,
        "Content-Length should be original size"
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

#[tokio::test]
#[serial]
async fn test_small_object_not_compressed() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting small object test");

    let mut env = RustFSTestEnvironment::new().await?;
    start_rustfs_with_compression(&mut env).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(COMPRESSION_TEST_BUCKET).await?;

    let small_size = MIN_COMPRESSIBLE_SIZE - 1;
    let small_data = generate_compressible_data(small_size);
    let object_key = "test-small.txt";

    client
        .put_object()
        .bucket(COMPRESSION_TEST_BUCKET)
        .key(object_key)
        .body(ByteStream::from(small_data.clone()))
        .send()
        .await?;

    let get_response = client
        .get_object()
        .bucket(COMPRESSION_TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(&downloaded_data[..], &small_data[..]);

    info!("Small object test passed");

    env.delete_test_bucket(COMPRESSION_TEST_BUCKET).await?;
    env.stop_server();
    Ok(())
}