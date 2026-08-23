#![cfg(test)]

use crate::common::{RustFSTestEnvironment, TEST_BUCKET, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use std::error::Error;
use std::fmt::Debug;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn assert_s3_error_code<T, E>(result: Result<T, SdkError<E>>, expected: &str)
where
    T: Debug,
    E: ProvideErrorMetadata + Debug,
{
    let error = result.expect_err("conditional request must fail");
    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some(expected),
        "unexpected conditional request error: {error:?}"
    );
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
async fn upload_object_with_metadata(
    client: &Client,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(Bytes::from(data.to_vec()).into())
        .send()
        .await?;

    response
        .e_tag()
        .map(str::to_owned)
        .ok_or_else(|| std::io::Error::other("put object response did not include an ETag").into())
}

async fn object_body(client: &Client, key: &str) -> Result<Bytes, Box<dyn Error + Send + Sync>> {
    let response = client.get_object().bucket(TEST_BUCKET).key(key).send().await?;
    Ok(response.body.collect().await?.into_bytes())
}

#[tokio::test]
async fn test_conditional_put_okay() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(TEST_BUCKET).await?;
    let client = env.create_s3_client();

    let test_key = "conditional-put-ok";
    let initial_data = generate_test_data(1024); // 1KB test data
    let matching_data = generate_test_data(2048); // 2KB updated data
    let non_matching_data = generate_test_data(3072); // 3KB updated data

    // Upload initial object and get its ETag
    let initial_etag = upload_object_with_metadata(&client, TEST_BUCKET, test_key, &initial_data).await?;

    // Test 1: PUT with matching If-Match condition (should succeed)
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .body(Bytes::from(matching_data.clone()).into())
        .if_match(&initial_etag)
        .send()
        .await?;
    assert_eq!(object_body(&client, test_key).await?.as_ref(), matching_data);

    // Test 2: PUT with non-matching If-None-Match condition (should succeed)
    let fake_etag = "\"fake-etag-12345\"";
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .body(Bytes::from(non_matching_data.clone()).into())
        .if_none_match(fake_etag)
        .send()
        .await?;
    assert_eq!(object_body(&client, test_key).await?.as_ref(), non_matching_data);

    Ok(())
}

#[tokio::test]
async fn test_conditional_put_failed() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(TEST_BUCKET).await?;
    let client = env.create_s3_client();

    let test_key = "conditional-put-failed";
    let initial_data = generate_test_data(1024);
    let updated_data = generate_test_data(2048);

    // Upload initial object and get its ETag
    let initial_etag = upload_object_with_metadata(&client, TEST_BUCKET, test_key, &initial_data).await?;

    // Test 1: PUT with non-matching If-Match condition (should fail with 412)
    let fake_etag = "\"fake-etag-should-not-match\"";
    let response1 = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_match(fake_etag)
        .send()
        .await;

    assert_s3_error_code(response1, "PreconditionFailed");
    assert_eq!(object_body(&client, test_key).await?.as_ref(), initial_data);

    // Test 2: PUT with matching If-None-Match condition (should fail with 412)
    let response2 = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .body(Bytes::from(updated_data.clone()).into())
        .if_none_match(&initial_etag)
        .send()
        .await;

    assert_s3_error_code(response2, "PreconditionFailed");
    assert_eq!(object_body(&client, test_key).await?.as_ref(), initial_data);

    Ok(())
}

#[tokio::test]
async fn test_conditional_put_when_object_does_not_exist() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(TEST_BUCKET).await?;
    let client = env.create_s3_client();

    let key = "conditional-put-missing";

    // When the object does not exist, the If-Match condition should always fail
    let response1 = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(Bytes::from(generate_test_data(1024)).into())
        .if_match("*")
        .send()
        .await;
    assert_s3_error_code(response1, "NoSuchKey");

    // When the object does not exist, the If-None-Match condition should be able to succeed
    let created_data = generate_test_data(1024);
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(Bytes::from(created_data.clone()).into())
        .if_none_match("*")
        .send()
        .await?;
    assert_eq!(object_body(&client, key).await?.as_ref(), created_data);

    Ok(())
}

#[tokio::test]
async fn test_conditional_multi_part_upload() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(TEST_BUCKET).await?;
    let client = env.create_s3_client();

    let test_key = "conditional-multipart-upload";
    let test_data = generate_test_data(1024);
    let initial_etag = upload_object_with_metadata(&client, TEST_BUCKET, test_key, &test_data).await?;

    let part_size = 5 * 1024 * 1024; // 5MB per part (minimum for multipart)
    let num_parts = 3;
    let mut parts = Vec::new();
    let mut expected_data = Vec::with_capacity(part_size * usize::try_from(num_parts)?);

    // Initiate multipart upload
    let initiate_response = client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .send()
        .await?;

    let upload_id = initiate_response
        .upload_id()
        .ok_or_else(|| std::io::Error::other("No upload ID returned"))?;

    // Upload parts
    for part_number in 1..=num_parts {
        let part_data = vec![u8::try_from(part_number)?; part_size];
        expected_data.extend_from_slice(&part_data);

        let upload_part_response = client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(test_key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(Bytes::from(part_data).into())
            .send()
            .await?;

        let part_etag = upload_part_response
            .e_tag()
            .ok_or_else(|| std::io::Error::other("Do not have etag"))?
            .to_string();

        let completed_part = CompletedPart::builder().part_number(part_number).e_tag(part_etag).build();

        parts.push(completed_part);
    }

    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();

    // Test 1: Multipart upload with wildcard If-None-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_none_match("*")
        .send()
        .await;

    assert_s3_error_code(complete_response, "PreconditionFailed");

    // Test 2: Multipart upload with matching If-None-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_none_match(initial_etag.clone())
        .send()
        .await;

    assert_s3_error_code(complete_response, "PreconditionFailed");

    // Test 3: Multipart upload with unmatching If-Match, should fail
    let complete_response = client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload.clone())
        .if_match("\"abcdef\"")
        .send()
        .await;

    assert_s3_error_code(complete_response, "PreconditionFailed");

    let staged_parts = client
        .list_parts()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .upload_id(upload_id)
        .send()
        .await?;
    assert_eq!(staged_parts.parts().len(), usize::try_from(num_parts)?);

    // Test 4: Multipart upload with matching If-Match, should succeed
    client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .if_match(initial_etag)
        .send()
        .await?;
    assert_eq!(object_body(&client, test_key).await?.as_ref(), expected_data);

    Ok(())
}
