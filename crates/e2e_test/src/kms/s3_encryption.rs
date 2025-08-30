#[allow(unused_imports)]
use super::{cleanup_test_context, setup_test_context};
#[allow(unused_imports)]
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule},
};
#[allow(unused_imports)]
use base64::{Engine, engine::general_purpose::STANDARD};
#[allow(unused_imports)]
use md5::{Digest, Md5};

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_s3_sse_s3_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-sse-s3-bucket";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with SSE-S3
    let test_data = b"Hello, SSE-S3 encryption!";
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    // Download and verify
    let response = client.get_object().bucket(bucket).key("test-object").send().await?;

    let downloaded_data = response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_s3_sse_kms_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-sse-kms-bucket";
    let kms_key_id = "test-kms-key";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with SSE-KMS
    let test_data = b"Hello, SSE-KMS encryption!";
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(kms_key_id)
        .send()
        .await?;

    // Download and verify
    let response = client.get_object().bucket(bucket).key("test-object").send().await?;

    let downloaded_data = response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_s3_sse_c_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-sse-c-bucket";
    let customer_key = b"1234567890abcdef1234567890abcdef"; // 32 bytes
    let mut hasher = Md5::new();
    hasher.update(customer_key);
    let customer_key_md5 = STANDARD.encode(hasher.finalize().as_slice());

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Upload object with SSE-C
    let test_data = b"Hello, SSE-C encryption!";
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(STANDARD.encode(customer_key))
        .sse_customer_key_md5(customer_key_md5.clone())
        .send()
        .await?;

    // Download and verify
    let response = client
        .get_object()
        .bucket(bucket)
        .key("test-object")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(STANDARD.encode(customer_key))
        .sse_customer_key_md5(customer_key_md5)
        .send()
        .await?;

    let downloaded_data = response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_bucket_default_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-default-encryption-bucket";

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

    // Upload object without encryption headers (should use default)
    let test_data = b"Hello, default encryption!";
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(ByteStream::from(test_data.to_vec()))
        .send()
        .await?;

    // Download and verify
    let response = client.get_object().bucket(bucket).key("test-object").send().await?;

    let downloaded_data = response.body.collect().await?.to_vec();
    assert_eq!(downloaded_data, test_data);

    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_multipart_upload_with_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let client = &test_context.s3_client;
    let bucket = "test-multipart-encryption-bucket";

    // Create bucket
    client.create_bucket().bucket(bucket).send().await?;

    // Create multipart upload with SSE-S3
    let multipart_upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-object")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    // Upload parts
    let part_data = vec![b'A'; 5 * 1024 * 1024]; // 5MB part
    let mut hasher = Md5::new();
    hasher.update(&part_data);
    let expected_hash = hasher.finalize();
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
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    let completed_part = CompletedPart::builder()
        .part_number(1)
        .e_tag(upload_part.e_tag().unwrap())
        .build();

    let completed_upload = CompletedMultipartUpload::builder().parts(completed_part).build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-object")
        .upload_id(multipart_upload.upload_id().unwrap())
        .multipart_upload(completed_upload)
        .send()
        .await?;

    // Verify upload
    let response = client.get_object().bucket(bucket).key("large-object").send().await?;

    let downloaded_data = response.body.collect().await?.to_vec();
    let mut downloaded_hasher = Md5::new();
    downloaded_hasher.update(&downloaded_data);
    let downloaded_hash = downloaded_hasher.finalize();
    assert_eq!(downloaded_hash, expected_hash);
    assert_eq!(downloaded_data.len(), 5 * 1024 * 1024);

    cleanup_test_context(test_context).await?;
    Ok(())
}
