#![cfg(test)]

use crate::common::{RustFSTestEnvironment, TEST_BUCKET, init_logging};
use crate::kms::common::LocalKMSTestEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, ServerSideEncryption};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use serial_test::serial;
use url::Url;

const BUCKET: &str = TEST_BUCKET;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Setup a dedicated RustFS environment for the append tests.
async fn setup_test_environment() -> TestResult<(RustFSTestEnvironment, Client)> {
    setup_test_environment_with_env(&[]).await
}

async fn setup_test_environment_with_env(extra_env: &[(&str, &str)]) -> TestResult<(RustFSTestEnvironment, Client)> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], extra_env).await?;
    env.create_test_bucket(BUCKET).await?;

    let client = env.create_s3_client();
    Ok((env, client))
}

/// Setup a RustFS environment with local KMS enabled for encryption scenarios.
async fn setup_kms_test_environment() -> TestResult<(LocalKMSTestEnvironment, Client)> {
    init_logging();

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    kms_env.start_rustfs_for_local_kms().await?;
    kms_env.base_env.create_test_bucket(BUCKET).await?;

    let client = kms_env.base_env.create_s3_client();
    Ok((kms_env, client))
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

/// Put object with custom header using low-level HTTP client
async fn put_object_with_append_header(
    env: &RustFSTestEnvironment,
    bucket: &str,
    key: &str,
    data: &[u8],
    write_offset: Option<i64>,
    extra_headers: &[(&str, &str)],
) -> TestResult<(u16, String)> {
    use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings};
    use aws_sigv4::sign::v4;
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    // Calculate SHA256 of the payload
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut payload_hash = vec![0u8; result.len() * 2];
    faster_hex::hex_encode(&result, &mut payload_hash).map_err(|e| format!("Hex encode error: {:?}", e))?;
    let payload_hash = String::from_utf8(payload_hash)?;

    let mut base_url = Url::parse(env.url.as_str()).map_err(|e| format!("Invalid endpoint URL: {}", e))?;
    if !base_url.path().ends_with('/') {
        let mut path = base_url.path().trim_end_matches('/').to_string();
        path.push('/');
        base_url.set_path(&path);
    }

    let object_path = format!("{bucket}/{key}");
    let url = base_url
        .join(&object_path)
        .map_err(|e| format!("Failed to construct request URL: {}", e))?;

    let host_header = match (url.host_str(), url.port_or_known_default()) {
        (Some(host), Some(port)) => {
            if url.scheme() == "http" && port == 80 || url.scheme() == "https" && port == 443 {
                host.to_string()
            } else {
                format!("{host}:{port}")
            }
        }
        (Some(host), None) => host.to_string(),
        _ => return Err("Endpoint URL missing host".into()),
    };

    let mut request = http::Request::builder()
        .method("PUT")
        .uri(url.as_str())
        .header("host", host_header)
        .header("content-length", data.len().to_string())
        .header("x-amz-content-sha256", &payload_hash);

    // Add append header if specified
    if let Some(offset) = write_offset {
        request = request.header("x-amz-write-offset-bytes", offset.to_string());
    }

    for (name, value) in extra_headers {
        request = request.header(*name, *value);
    }

    let mut request = request.body(()).map_err(|e| format!("Failed to build request: {}", e))?;

    // Sign the request using AWS SigV4
    let credentials =
        aws_credential_types::Credentials::new(env.access_key.as_str(), env.secret_key.as_str(), None, None, "static");

    let identity = credentials.into();
    let signing_settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .map_err(|e| format!("Failed to build signing params: {}", e))?
        .into();

    let signable_request = SignableRequest::new(
        request.method().as_str(),
        request.uri().to_string(),
        request.headers().iter().map(|(k, v)| (k.as_str(), v.to_str().unwrap())),
        SignableBody::Bytes(data),
    )
    .map_err(|e| format!("Failed to create signable request: {}", e))?;

    let (signing_instructions, _) = aws_sigv4::http_request::sign(signable_request, &signing_params)
        .map_err(|e| format!("Failed to sign request: {}", e))?
        .into_parts();

    // Apply signing instructions to request
    signing_instructions.apply_to_request_http1x(&mut request);

    // Send the request using reqwest
    let client = reqwest::Client::builder().no_proxy().build()?;
    let mut req_builder = client.request(request.method().clone(), request.uri().to_string());

    // Copy headers from signed request
    for (key, value) in request.headers() {
        req_builder = req_builder.header(key, value);
    }

    // Add body
    req_builder = req_builder.body(data.to_vec());

    let response = req_builder.send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;

    Ok((status, body))
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_basic() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;

    let test_key = generate_test_key("append-basic");

    // Step 1: Create initial object with 1KB data
    let initial_data = generate_test_data(1024);
    println!("Creating initial object with {} bytes", initial_data.len());
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(ByteStream::from(Bytes::from(initial_data.clone())))
        .send()
        .await?;

    println!("Initial object created successfully");

    // Verify initial object
    println!("Verifying initial object...");
    let get_response = client.get_object().bucket(BUCKET).key(&test_key).send().await?;
    let initial_body = get_response.body.collect().await?;
    let initial_body = initial_body.into_bytes();
    println!("Retrieved {} bytes", initial_body.len());
    assert_eq!(initial_body.len(), 1024);
    assert_eq!(initial_body.to_vec(), initial_data);

    // Step 2: Append 512 bytes at offset 1024
    let append_data = generate_test_data(512);
    let (status, _body) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(1024), &[]).await?;

    assert_eq!(status, 200, "Append operation should succeed with status 200");

    // Step 3: Read the complete object and verify
    let get_response = client.get_object().bucket(BUCKET).key(&test_key).send().await?;
    let final_body = get_response.body.collect().await?.into_bytes();

    assert_eq!(final_body.len(), 1536, "Final object should be 1536 bytes (1024 + 512)");

    // Verify first part
    assert_eq!(&final_body[..1024], &initial_data[..]);

    // Verify appended part
    assert_eq!(&final_body[1024..], &append_data[..]);

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_wrong_offset() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;

    let test_key = generate_test_key("append-wrong-offset");

    // Create initial object with 1KB data
    let initial_data = generate_test_data(1024);
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(ByteStream::from(Bytes::from(initial_data)))
        .send()
        .await?;

    // Try to append at wrong offset (should fail)
    let append_data = generate_test_data(512);
    let (status, body) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(2048), &[]).await?;

    assert!(
        (400..500).contains(&status),
        "Append with wrong offset should fail with 4xx error, got: {} body: {}",
        status,
        body
    );

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_multiple_times() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;

    let test_key = generate_test_key("append-multiple");

    // Create initial object
    let initial_data = generate_test_data(1024);
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(ByteStream::from(Bytes::from(initial_data.clone())))
        .send()
        .await?;

    let mut expected_data = initial_data.clone();
    let mut current_offset = 1024i64;

    // Append 3 more times
    for i in 1..=3 {
        let append_data = generate_test_data(512);

        let (status, _) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(current_offset), &[]).await?;

        assert_eq!(status, 200, "Append #{} should succeed", i);

        expected_data.extend_from_slice(&append_data);
        current_offset += 512;
    }

    // Verify final object
    let get_response = client.get_object().bucket(BUCKET).key(&test_key).send().await?;
    let final_body = get_response.body.collect().await?.into_bytes();

    assert_eq!(final_body.len(), 2560, "Final object should be 2560 bytes (1024 + 3*512)");
    assert_eq!(final_body.to_vec(), expected_data);

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_to_nonexistent_object() -> TestResult<()> {
    let (env, _client) = setup_test_environment().await?;

    let test_key = generate_test_key("append-nonexistent");

    // Try to append to non-existent object with non-zero offset
    let append_data = generate_test_data(512);
    let (status, body) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(512), &[]).await?;

    assert!(
        (400..500).contains(&status),
        "Append to non-existent object with non-zero offset should fail with 4xx error, got: {} body: {}",
        status,
        body
    );

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_large_data() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;

    let test_key = generate_test_key("append-large");

    // Create initial object with 5MB data
    let initial_data = generate_test_data(5 * 1024 * 1024);
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(ByteStream::from(Bytes::from(initial_data.clone())))
        .send()
        .await?;

    // Append another 5MB
    let append_data = generate_test_data(5 * 1024 * 1024);
    let (status, _) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(5 * 1024 * 1024), &[]).await?;

    assert_eq!(status, 200, "Append large data should succeed");

    // Verify final object size
    let get_response = client.get_object().bucket(BUCKET).key(&test_key).send().await?;
    let final_body = get_response.body.collect().await?.into_bytes();

    assert_eq!(final_body.len(), 10 * 1024 * 1024, "Final object should be 10MB");

    // Cleanup
    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_rejects_sse_s3_object() -> TestResult<()> {
    let (kms_env, client) = setup_kms_test_environment().await?;
    let env = &kms_env.base_env;

    let test_key = generate_test_key("append-sse-s3");
    let initial_data = generate_test_data(2048);

    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(Bytes::from(initial_data.clone())))
        .send()
        .await?;

    let append_data = generate_test_data(512);
    let (status, body) = put_object_with_append_header(
        env,
        BUCKET,
        &test_key,
        &append_data,
        Some(initial_data.len() as i64),
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await?;

    assert!(
        (400..500).contains(&status),
        "Append to SSE-S3 object should fail, got status {} body {}",
        status,
        body
    );

    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_rejects_sse_c_object() -> TestResult<()> {
    let (kms_env, client) = setup_kms_test_environment().await?;
    let env = &kms_env.base_env;

    let test_key = generate_test_key("append-sse-c");
    let customer_key = [0x11u8; 32];
    let key_b64 = BASE64_STANDARD.encode(customer_key);
    let key_md5 = format!("{:x}", md5::compute(customer_key));

    let initial_data = generate_test_data(2048);
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(key_b64.as_str())
        .sse_customer_key_md5(key_md5.as_str())
        .body(ByteStream::from(Bytes::from(initial_data.clone())))
        .send()
        .await?;

    let append_data = generate_test_data(256);
    let (status, body) = put_object_with_append_header(
        env,
        BUCKET,
        &test_key,
        &append_data,
        Some(initial_data.len() as i64),
        &[
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key", key_b64.as_str()),
            ("x-amz-server-side-encryption-customer-key-MD5", key_md5.as_str()),
        ],
    )
    .await?;

    assert!(
        (400..500).contains(&status),
        "Append to SSE-C object should fail, got status {} body {}",
        status,
        body
    );

    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_rejects_compressed_object() -> TestResult<()> {
    let (env, client) = setup_test_environment_with_env(&[(rustfs_ecstore::compress::ENV_COMPRESSION_ENABLED, "true")]).await?;

    let test_key = generate_test_key("append-compressed");
    let initial_data = generate_test_data(8 * 1024);

    let compression_metadata_value = "zstd".to_string();
    let actual_size_value = initial_data.len().to_string();
    let compression_headers = [
        ("content-type", "text/plain"),
        ("x-rustfs-meta-x-rustfs-internal-compression", compression_metadata_value.as_str()),
        ("x-rustfs-meta-x-rustfs-internal-actual-size", actual_size_value.as_str()),
    ];

    let (status, body) =
        put_object_with_append_header(&env, BUCKET, &test_key, &initial_data, None, &compression_headers).await?;
    assert_eq!(status, 200, "Initial compressed PUT should succeed, got status {} body {}", status, body);

    let append_data = generate_test_data(1024);
    let (status, body) =
        put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(initial_data.len() as i64), &[]).await?;

    assert!(
        (400..500).contains(&status),
        "Append to compressed object should fail, got status {} body {}",
        status,
        body
    );

    cleanup_objects(&client, BUCKET, &[&test_key]).await;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_respects_existing_part_numbers() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;
    let test_key = generate_test_key("append-gap-parts");

    let create_resp = client.create_multipart_upload().bucket(BUCKET).key(&test_key).send().await?;
    let upload_id = create_resp.upload_id().ok_or("missing upload id")?.to_string();

    let part1 = vec![b'A'; 5 * 1024 * 1024];
    let part3 = vec![b'B'; 5 * 1024 * 1024];

    let part1_resp = client
        .upload_part()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id.clone())
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await?;
    let part3_resp = client
        .upload_part()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id.clone())
        .part_number(3)
        .body(ByteStream::from(part3.clone()))
        .send()
        .await?;

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .set_part_number(Some(1))
                .set_e_tag(part1_resp.e_tag().map(|s| s.to_string()))
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .set_part_number(Some(3))
                .set_e_tag(part3_resp.e_tag().map(|s| s.to_string()))
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&test_key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    let append_data = vec![b'C'; 1024];
    let append_offset = (part1.len() + part3.len()) as i64;
    let (status, body) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(append_offset), &[]).await?;
    assert_eq!(status, 200, "Append should succeed but got status {} body {}", status, body);

    let final_data = client
        .get_object()
        .bucket(BUCKET)
        .key(&test_key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();

    let mut expected = Vec::new();
    expected.extend_from_slice(&part1);
    expected.extend_from_slice(&part3);
    expected.extend_from_slice(&append_data);

    assert_eq!(final_data.len(), expected.len());
    assert_eq!(final_data.to_vec(), expected);

    cleanup_objects(&client, BUCKET, &[&test_key]).await;
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_with_zero_write_offset_header() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;
    let test_key = generate_test_key("append-offset-zero");

    let data = generate_test_data(2048);
    let (status, body) = put_object_with_append_header(&env, BUCKET, &test_key, &data, Some(0), &[]).await?;
    assert_eq!(status, 200, "Initial PUT with zero offset should succeed, body {}", body);

    let fetched = client
        .get_object()
        .bucket(BUCKET)
        .key(&test_key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(fetched.len(), data.len());
    assert_eq!(fetched.to_vec(), data);

    cleanup_objects(&client, BUCKET, &[&test_key]).await;
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_append_zero_offset_on_existing_object_fails() -> TestResult<()> {
    let (env, client) = setup_test_environment().await?;
    let test_key = generate_test_key("append-offset-zero-existing");

    let base_data = generate_test_data(1024);
    client
        .put_object()
        .bucket(BUCKET)
        .key(&test_key)
        .body(ByteStream::from(Bytes::from(base_data.clone())))
        .send()
        .await?;

    let append_data = generate_test_data(512);
    let (status, body) = put_object_with_append_header(&env, BUCKET, &test_key, &append_data, Some(0), &[]).await?;
    assert!(
        (400..500).contains(&status),
        "Append with zero offset on existing object should fail, got status {} body {}",
        status,
        body
    );

    cleanup_objects(&client, BUCKET, &[&test_key]).await;
    Ok(())
}
