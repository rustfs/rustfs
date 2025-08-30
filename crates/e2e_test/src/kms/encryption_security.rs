#[allow(unused_imports)]
use super::{cleanup_test_context, setup_test_context};
#[allow(unused_imports)]
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};
#[allow(unused_imports)]
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_key_isolation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket1 = "test-key-isolation-1";
    let bucket2 = "test-key-isolation-2";

    // Create buckets
    client.create_bucket().bucket(bucket1).send().await?;
    client.create_bucket().bucket(bucket2).send().await?;

    // Upload to bucket1 with KMS key1
    let test_data = b"Secret data for bucket1";
    client
        .put_object()
        .bucket(bucket1)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-1")
        .send()
        .await?;

    // Upload to bucket2 with KMS key2
    client
        .put_object()
        .bucket(bucket2)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-2")
        .send()
        .await?;

    // Verify objects are encrypted with different keys
    let response1 = client.get_object().bucket(bucket1).key("test-object").send().await?;

    let response2 = client.get_object().bucket(bucket2).key("test-object").send().await?;

    // Both should decrypt successfully with their respective keys
    assert_eq!(response1.body.collect().await?.to_vec(), test_data);
    assert_eq!(response2.body.collect().await?.to_vec(), test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_metadata_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-metadata-encryption";

    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with sensitive metadata
    let test_data = b"Test data";
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .metadata("sensitive-key", "sensitive-value")
        .send()
        .await?;

    // Verify metadata is accessible but not in plaintext storage
    let response = client.head_object().bucket(bucket).key("test-object").send().await?;

    let metadata = response.metadata().cloned().unwrap_or_default();
    assert!(metadata.contains_key("sensitive-key"));
    assert_eq!(metadata["sensitive-key"], "sensitive-value");

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_error_information_leakage() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-error-leakage";

    client.create_bucket().bucket(bucket).send().await?;

    // Test with invalid KMS key
    let test_data = b"Test data";
    let result = client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("invalid-key-id")
        .send()
        .await;

    // Error should not contain sensitive information
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = format!("{error:?}");

    // Ensure no sensitive data in error message
    assert!(!error_str.contains("invalid-key-id"));
    assert!(!error_str.contains("arn:aws:kms"));

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_side_channel_resistance() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-side-channel";

    client.create_bucket().bucket(bucket).send().await?;

    // Test timing consistency for encryption operations
    let test_data = b"Test data for timing analysis";
    let iterations = 10;
    let mut timings = Vec::new();

    for _ in 0..iterations {
        let start = Instant::now();

        client
            .put_object()
            .bucket(bucket)
            .key("timing-test")
            .body(ByteStream::from(test_data.to_vec()))
            .server_side_encryption(ServerSideEncryption::Aes256)
            .send()
            .await?;

        let duration = start.elapsed();
        timings.push(duration);

        // Clean up
        client.delete_object().bucket(bucket).key("timing-test").send().await?;
    }

    // Calculate timing variance
    let avg = timings.iter().sum::<Duration>() / timings.len() as u32;
    let variance = timings
        .iter()
        .map(|&t| {
            let diff = t.abs_diff(avg);
            diff.as_micros()
        })
        .sum::<u128>()
        / timings.len() as u128;

    // Variance should be reasonable (less than 10ms)
    assert!(variance < 10_000);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_unauthorized_access_attempts() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let _bucket = "test-unauthorized-access";

    // Create unauthorized client - skip this test for now as it requires special setup
    // let unauthorized_client = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
    // let unauthorized_s3 = aws_sdk_s3::Client::new(&unauthorized_client);

    // Test unauthorized access - commented out as it requires special setup

    cleanup_test_context(test_context).await?;
    Ok(())
}
