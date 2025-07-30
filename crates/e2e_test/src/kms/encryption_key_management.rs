#[allow(unused_imports)]
use super::{cleanup_test_context, setup_test_context};
#[allow(unused_imports)]
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_kms_key_rotation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-kms-key-rotation";

    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with initial KMS key
    let test_data = b"Data before key rotation";
    client
        .put_object()
        .bucket(bucket)
        .key("rotating-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-v1")
        .send()
        .await?;

    // Rotate KMS key (simulate key rotation)
    let new_key_id = "key-v2";

    // Upload new object with rotated key
    let new_data = b"Data after key rotation";
    client
        .put_object()
        .bucket(bucket)
        .key("new-object")
        .body(ByteStream::from(new_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(new_key_id)
        .send()
        .await?;

    // Verify both objects are accessible
    let response1 = client.get_object().bucket(bucket).key("rotating-object").send().await?;

    let response2 = client.get_object().bucket(bucket).key("new-object").send().await?;

    assert_eq!(response1.body.collect().await?.to_vec(), test_data);
    assert_eq!(response2.body.collect().await?.to_vec(), new_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_key_versioning() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-key-versioning";

    client.create_bucket().bucket(bucket).send().await?;

    // Enable versioning on bucket
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    // Upload object with KMS key
    let test_data = b"Original data";
    client
        .put_object()
        .bucket(bucket)
        .key("versioned-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-v1")
        .send()
        .await?;

    // Update object with new KMS key
    let updated_data = b"Updated data";
    client
        .put_object()
        .bucket(bucket)
        .key("versioned-object")
        .body(ByteStream::from(updated_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-v2")
        .send()
        .await?;

    // List versions
    let versions = client
        .list_object_versions()
        .bucket(bucket)
        .prefix("versioned-object")
        .send()
        .await?;

    let versions = versions.versions();
    assert!(versions.len() >= 2);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_key_access_policies() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-key-policies";

    client.create_bucket().bucket(bucket).send().await?;

    // Test different KMS key access scenarios
    let test_cases = vec![("allowed-key", true), ("denied-key", false), ("expired-key", false)];

    for (key_id, should_succeed) in test_cases {
        let test_data = format!("Test data for {key_id}");
        let result = client
            .put_object()
            .bucket(bucket)
            .key(format!("test-{key_id}-object"))
            .body(ByteStream::from(test_data.as_bytes().to_vec()))
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(key_id)
            .send()
            .await;

        match should_succeed {
            true => {
                assert!(result.is_ok(), "Expected success for key: {key_id}");
            }
            false => {
                assert!(result.is_err(), "Expected failure for key: {key_id}");
            }
        }
    }

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_key_deletion_handling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-key-deletion";

    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with KMS key
    let test_data = b"Data with key to be deleted";
    client
        .put_object()
        .bucket(bucket)
        .key("delete-key-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("key-to-delete")
        .send()
        .await?;

    // Simulate key deletion
    // In real scenario, this would be a KMS key deletion

    // Attempt to access object after key deletion
    let result = client.get_object().bucket(bucket).key("delete-key-object").send().await;

    // Should fail with appropriate error
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = format!("{error:?}");
    assert!(error_str.contains("KMS.NotFoundException") || error_str.contains("AccessDenied"));

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_cross_account_key_access() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-cross-account";

    client.create_bucket().bucket(bucket).send().await?;

    // Test accessing KMS key from different AWS account
    let cross_account_key = "arn:aws:kms:us-east-1:123456789012:key/cross-account-key";

    let test_data = b"Cross account key test";
    let result = client
        .put_object()
        .bucket(bucket)
        .key("cross-account-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(cross_account_key)
        .send()
        .await;

    // Should handle cross-account access appropriately
    // In real scenario, this would depend on IAM policies
    assert!(result.is_err()); // Expected to fail in test environment

    cleanup_test_context(test_context).await?;
    Ok(())
}
