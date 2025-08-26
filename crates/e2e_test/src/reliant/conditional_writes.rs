#![cfg(test)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use serial_test::serial;
use std::error::Error;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "api-test";

async fn create_aws_s3_client() -> Result<Client, Box<dyn Error>> {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "static"))
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    Ok(client)
}

/// Setup test bucket, creating it if it doesn't exist
async fn setup_test_bucket(client: &Client) -> Result<(), Box<dyn Error>> {
    match client.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(SdkError::ServiceError(e)) => {
            let e = e.into_err();
            let error_code = e.meta().code().unwrap_or("");
            if !error_code.eq("BucketAlreadyExists") {
                return Err(e.into());
            }
        }
        Err(e) => {
            return Err(e.into());
        }
    }
    Ok(())
}

/// Generate test data of specified size
fn generate_test_data(size: usize) -> Vec<u8> {
    let pattern = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push(pattern[i % pattern.len()]);
    }
    data
}

/// Upload an object and return its ETag
async fn upload_object_with_metadata(client: &Client, bucket: &str, key: &str, data: &[u8]) -> Result<String, Box<dyn Error>> {
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(Bytes::from(data.to_vec()).into())
        .send()
        .await?;

    let etag = response.e_tag().unwrap_or("").to_string();
    Ok(etag)
}

/// Cleanup test objects from bucket
async fn cleanup_objects(client: &Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
}

/// Generate unique test object key
fn generate_test_key(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{prefix}-{timestamp}")
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_conditional_put_okay() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let test_key = generate_test_key("conditional-put-ok");
    let initial_data = generate_test_data(1024); // 1KB test data
    let updated_data = generate_test_data(2048); // 2KB updated data

    // Upload initial object and get its ETag
    let initial_etag = upload_object_with_metadata(&client, BUCKET, &test_key, &initial_data).await?;

    // Test 1: PUT with matching If-Match condition (should succeed)
    let response1 = client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_match(&initial_etag)
        .send()
        .await;
    assert!(response1.is_ok(), "PUT with matching If-Match should succeed");

    // Test 2: PUT with non-matching If-None-Match condition (should succeed)
    let fake_etag = "\"fake-etag-12345\"";
    let response2 = client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_none_match(fake_etag)
        .send()
        .await;
    assert!(response2.is_ok(), "PUT with non-matching If-None-Match should succeed");

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_conditional_put_failed() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let test_key = generate_test_key("conditional-put-failed");
    let initial_data = generate_test_data(1024);
    let updated_data = generate_test_data(2048);

    // Upload initial object and get its ETag
    let initial_etag = upload_object_with_metadata(&client, BUCKET, &test_key, &initial_data).await?;

    // Test 1: PUT with non-matching If-Match condition (should fail with 412)
    let fake_etag = "\"fake-etag-should-not-match\"";
    let response1 = client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_match(fake_etag)
        .send()
        .await;

    assert!(response1.is_err(), "PUT with non-matching If-Match should fail");
    if let Err(e) = response1 {
        if let SdkError::ServiceError(e) = e {
            let e = e.into_err();
            let error_code = e.meta().code().unwrap_or("");
            assert_eq!("PreconditionFailed", error_code);
        } else {
            panic!("Unexpected error: {e:?}");
        }
    }

    // Test 2: PUT with matching If-None-Match condition (should fail with 412)
    let response2 = client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_none_match(&initial_etag)
        .send()
        .await;

    assert!(response2.is_err(), "PUT with matching If-None-Match should fail");
    if let Err(e) = response2 {
        if let SdkError::ServiceError(e) = e {
            let e = e.into_err();
            let error_code = e.meta().code().unwrap_or("");
            assert_eq!("PreconditionFailed", error_code);
        } else {
            panic!("Unexpected error: {e:?}");
        }
    }

    // Cleanup - only need to clean up the initial object since failed PUTs shouldn't create objects
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_conditional_put_when_object_does_not_exist() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let key = "some_key";
    cleanup_objects(&client, BUCKET, &[key]).await;

    // When the object does not exist, the If-Match condition should always fail
    let response1 = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(generate_test_data(1024)).into())
        .if_match("*")
        .send()
        .await;
    assert!(response1.is_err());
    if let Err(e) = response1 {
        if let SdkError::ServiceError(e) = e {
            let e = e.into_err();
            let error_code = e.meta().code().unwrap_or("");
            assert_eq!("NoSuchKey", error_code);
        } else {
            panic!("Unexpected error: {e:?}");
        }
    }

    // When the object does not exist, the If-None-Match condition should be able to succeed
    let response2 = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(generate_test_data(1024)).into())
        .if_none_match("*")
        .send()
        .await;
    assert!(response2.is_ok());

    cleanup_objects(&client, BUCKET, &[key]).await;
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_conditional_multi_part_upload() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let test_key = generate_test_key("multipart-upload-ok");
    let test_data = generate_test_data(1024);
    let initial_etag = upload_object_with_metadata(&client, BUCKET, &test_key, &test_data).await?;

    let part_size = 5 * 1024 * 1024; // 5MB per part (minimum for multipart)
    let num_parts = 3;
    let mut parts = Vec::new();

    // Initiate multipart upload
    let initiate_response = client.create_multipart_upload().bucket(BUCKET).key(&test_key).send().await?;

    let upload_id = initiate_response
        .upload_id()
        .ok_or(std::io::Error::other("No upload ID returned"))?;

    // Upload parts
    for part_number in 1..=num_parts {
        let part_data = generate_test_data(part_size);

        let upload_part_response = client
            .upload_part()
            .bucket(BUCKET)
            .key(&test_key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(Bytes::from(part_data).into())
            .send()
            .await?;

        let part_etag = upload_part_response
            .e_tag()
            .ok_or(std::io::Error::other("Do not have etag"))?
            .to_string();

        let completed_part = CompletedPart::builder().part_number(part_number).e_tag(part_etag).build();

        parts.push(completed_part);
    }

    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();

    // Test 1: Multipart upload with wildcard If-None-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_none_match("*")
        .send()
        .await;

    assert!(complete_response.is_err());

    // Test 2: Multipart upload with matching If-None-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_none_match(initial_etag.clone())
        .send()
        .await;

    assert!(complete_response.is_err());

    // Test 3: Multipart upload with unmatching If-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_match("\"abcdef\"")
        .send()
        .await;

    assert!(complete_response.is_err());

    // Test 4: Multipart upload with matching If-Match, should succeed
    let complete_response = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_match(initial_etag)
        .send()
        .await;

    assert!(complete_response.is_ok());

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}
