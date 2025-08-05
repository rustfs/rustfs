//! Bucket encryption configuration tests
//!
//! This module tests bucket-level encryption configuration including:
//! - Setting and getting bucket encryption configuration
//! - Default encryption with SSE-S3 and SSE-KMS
//! - Bucket encryption inheritance for objects
//! - Encryption configuration validation

#[allow(unused_imports)]
use super::{cleanup_test_context, setup_test_context};
#[allow(unused_imports)]
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{
        CompletedMultipartUpload, CompletedPart, ServerSideEncryption, ServerSideEncryptionByDefault,
        ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    },
};
#[allow(unused_imports)]
use base64::{Engine, engine::general_purpose::STANDARD};

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_bucket_encryption_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-bucket-encryption-sse-s3";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption with SSE-S3
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    // Set bucket encryption
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Get bucket encryption and verify
    let response = client.get_bucket_encryption().bucket(bucket).send().await?;
    let config = response.server_side_encryption_configuration().unwrap();
    let rule = config.rules().first().unwrap();
    let default_encryption = rule.apply_server_side_encryption_by_default().unwrap();

    assert_eq!(default_encryption.sse_algorithm(), &ServerSideEncryption::Aes256);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_bucket_encryption_sse_kms() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-bucket-encryption-sse-kms";
    let kms_key_id = "test-kms-key-id";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption with SSE-KMS
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::AwsKms)
        .kms_master_key_id(kms_key_id)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    // Set bucket encryption
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Get bucket encryption and verify
    let response = client.get_bucket_encryption().bucket(bucket).send().await?;
    let config = response.server_side_encryption_configuration().unwrap();
    let rule = config.rules().first().unwrap();
    let default_encryption = rule.apply_server_side_encryption_by_default().unwrap();

    assert_eq!(default_encryption.sse_algorithm(), &ServerSideEncryption::AwsKms);
    assert_eq!(default_encryption.kms_master_key_id().unwrap(), kms_key_id);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_bucket_default_encryption_inheritance() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-bucket-encryption-inheritance";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Upload object without explicit encryption (should inherit bucket default)
    let test_data = b"Hello, bucket default encryption!";
    let put_response = client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .send()
        .await?;

    // Verify object was encrypted with bucket default
    assert!(put_response.server_side_encryption().is_some());

    // Download and verify data integrity
    let get_response = client.get_object().bucket(bucket).key("test-object").send().await?;
    let downloaded_data = get_response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_bucket_encryption_override_with_request_headers() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-bucket-encryption-override";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption with SSE-S3
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Upload object with explicit SSE-KMS (should override bucket default)
    let test_data = b"Hello, request header override!";
    let kms_key_id = "override-kms-key";

    let put_response = client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(kms_key_id)
        .send()
        .await?;

    // Verify object was encrypted with request header settings
    assert_eq!(put_response.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
    assert_eq!(put_response.ssekms_key_id(), Some(kms_key_id));

    // Download and verify data integrity
    let get_response = client.get_object().bucket(bucket).key("test-object").send().await?;
    let downloaded_data = get_response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_delete_bucket_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-delete-bucket-encryption";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Verify encryption is set
    let response = client.get_bucket_encryption().bucket(bucket).send().await?;
    assert!(response.server_side_encryption_configuration().is_some());

    // Delete bucket encryption
    client.delete_bucket_encryption().bucket(bucket).send().await?;

    // Verify encryption is removed (should return error or empty config)
    let result = client.get_bucket_encryption().bucket(bucket).send().await;
    assert!(result.is_err() || result.unwrap().server_side_encryption_configuration().is_none());

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_multipart_upload_with_bucket_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-multipart-bucket-encryption";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Configure bucket default encryption
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Create multipart upload without explicit encryption (should use bucket default)
    let multipart_upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-object")
        .send()
        .await?;

    // Upload parts
    let part_data = vec![b'B'; 5 * 1024 * 1024]; // 5MB part
    let upload_part = client
        .upload_part()
        .bucket(bucket)
        .key("large-object")
        .upload_id(multipart_upload.upload_id().unwrap())
        .part_number(1)
        .body(ByteStream::from(part_data.clone()))
        .send()
        .await?;

    // Complete multipart upload
    let completed_part = CompletedPart::builder()
        .part_number(1)
        .e_tag(upload_part.e_tag().unwrap())
        .build();

    let completed_upload = CompletedMultipartUpload::builder().parts(completed_part).build();

    let complete_response = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-object")
        .upload_id(multipart_upload.upload_id().unwrap())
        .multipart_upload(completed_upload)
        .send()
        .await?;

    // Verify object was encrypted with bucket default
    assert!(complete_response.server_side_encryption().is_some());

    // Download and verify data integrity
    let get_response = client.get_object().bucket(bucket).key("large-object").send().await?;
    let downloaded_data = get_response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data.len(), 5 * 1024 * 1024);
    assert_eq!(downloaded_data, part_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_copy_object_with_bucket_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let source_bucket = "test-copy-source-bucket";
    let dest_bucket = "test-copy-dest-bucket";

    // Create source and destination buckets
    client.create_bucket().bucket(source_bucket).send().await?;
    client.create_bucket().bucket(dest_bucket).send().await?;

    // Configure destination bucket with default encryption
    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();

    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();

    client
        .put_bucket_encryption()
        .bucket(dest_bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // Upload source object without encryption
    let test_data = b"Hello, copy with bucket encryption!";
    client
        .put_object()
        .bucket(source_bucket)
        .key("source-object")
        .body(ByteStream::from(test_data.to_vec()))
        .send()
        .await?;

    // Copy object to destination bucket (should inherit destination bucket encryption)
    let copy_source = format!("{}/{}", source_bucket, "source-object");
    let copy_response = client
        .copy_object()
        .bucket(dest_bucket)
        .key("dest-object")
        .copy_source(&copy_source)
        .send()
        .await?;

    // Verify copied object was encrypted with destination bucket default
    assert!(copy_response.server_side_encryption().is_some());

    // Download and verify data integrity
    let get_response = client.get_object().bucket(dest_bucket).key("dest-object").send().await?;
    let downloaded_data = get_response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}
