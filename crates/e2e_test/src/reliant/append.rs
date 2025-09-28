#![cfg(test)]

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use http::{
    HeaderValue,
    header::{IF_MATCH, IF_NONE_MATCH},
};
use serial_test::serial;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

async fn append_object(
    client: &Client,
    bucket: &str,
    key: &str,
    position: i64,
    payload: &[u8],
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    append_object_with_conditions(client, bucket, key, position, payload, None, None).await
}

async fn append_object_with_if_match(
    client: &Client,
    bucket: &str,
    key: &str,
    position: i64,
    payload: &[u8],
    if_match: Option<String>,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    append_object_with_conditions(client, bucket, key, position, payload, if_match, None).await
}

async fn append_object_with_if_none_match(
    client: &Client,
    bucket: &str,
    key: &str,
    position: i64,
    payload: &[u8],
    if_none_match: Option<String>,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    append_object_with_conditions(client, bucket, key, position, payload, None, if_none_match).await
}

async fn append_object_with_conditions(
    client: &Client,
    bucket: &str,
    key: &str,
    position: i64,
    payload: &[u8],
    if_match: Option<String>,
    if_none_match: Option<String>,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let if_match_header = if_match.clone();
    let if_none_match_header = if_none_match.clone();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.to_vec()))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut()
                .insert("x-amz-object-append", HeaderValue::from_static("true"));
            req.headers_mut().insert(
                "x-amz-append-position",
                HeaderValue::from_str(&position.to_string()).expect("invalid position header"),
            );
            if let Some(tag) = if_match_header.as_deref() {
                req.headers_mut()
                    .insert(IF_MATCH, HeaderValue::from_str(tag).expect("invalid if-match header"));
            }
            if let Some(tag) = if_none_match_header.as_deref() {
                req.headers_mut()
                    .insert(IF_NONE_MATCH, HeaderValue::from_str(tag).expect("invalid if-none-match header"));
            }
        })
        .send()
        .await
}

async fn append_action(
    client: &Client,
    bucket: &str,
    key: &str,
    action: &str,
    if_match: Option<&str>,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let action_value = HeaderValue::from_str(action).expect("invalid append action");
    let if_match_value = if_match.map(|v| HeaderValue::from_str(v).expect("invalid if-match"));
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b""))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-append-action", action_value.clone());
            if let Some(val) = if_match_value.as_ref() {
                req.headers_mut().insert(IF_MATCH, val.clone());
            }
        })
        .send()
        .await
}

fn md5_hex(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}

fn multipart_etag(etags: &[&str]) -> String {
    let mut buf = Vec::new();

    for etag in etags {
        let clean = etag.trim_matches('"');
        if clean.len() == 32 && clean.chars().all(|c| c.is_ascii_hexdigit()) {
            let mut chunk = Vec::with_capacity(clean.len() / 2);
            for i in (0..clean.len()).step_by(2) {
                let byte = u8::from_str_radix(&clean[i..i + 2], 16).expect("invalid hex");
                chunk.push(byte);
            }
            buf.extend_from_slice(&chunk);
        } else {
            buf.extend_from_slice(clean.as_bytes());
        }
    }

    let digest = md5::compute(buf);
    format!("{:x}-{}", digest, etags.len())
}

#[tokio::test]
#[serial]
async fn append_inline_object_updates_content_and_etag() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-inline-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-success.txt";
    let initial = b"hello";
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;

    let initial_fetch = client.get_object().bucket(&bucket).key(key).send().await?;
    let initial_body = initial_fetch.body.collect().await?.into_bytes();
    println!("initial body = {:?}", initial_body);

    let append_payload = b" world";
    append_object(&client, &bucket, key, initial.len() as i64, append_payload)
        .await
        .expect("append request should succeed");

    let second_payload = b"!!!";
    append_object(&client, &bucket, key, (initial.len() + append_payload.len()) as i64, second_payload)
        .await
        .expect("second append request should succeed");

    let expected: Vec<u8> = [initial.as_slice(), append_payload.as_slice(), second_payload.as_slice()].concat();
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let etag = get_resp.e_tag().map(|v| v.to_string());
    let aggregated = get_resp.body.collect().await?;
    let _body = aggregated.into_bytes();
    assert_eq!(_body.as_ref(), expected.as_slice());

    if let Some(etag) = etag {
        assert_eq!(etag.trim_matches('"'), md5_hex(&expected));
    } else {
        panic!("Append GET response missing ETag");
    }

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_inline_object_rejects_wrong_position() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-inline-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-mismatch.txt";
    let initial = b"abcdef";
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;

    let err = append_object(&client, &bucket, key, (initial.len() as i64) + 1, b"xyz")
        .await
        .expect_err("append with wrong position must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 400);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial);

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_segmented_object_appends_new_part() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-segmented-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-large.bin";

    let initial_size = 512 * 1024;
    let initial: Vec<u8> = (0..initial_size).map(|i| (i % 251) as u8).collect();
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.clone()))
        .send()
        .await?;
    let initial_etag = put_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("initial put etag");

    let append_payload: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 197) as u8).collect();
    let append_position = initial.len() as i64;

    let mut if_match = String::from("\"");
    if_match.push_str(&initial_etag);
    if_match.push('"');

    let append_resp = append_object_with_if_match(&client, &bucket, key, append_position, &append_payload, Some(if_match))
        .await
        .expect("append request must succeed");
    let append_etag = append_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("append response etag");

    let second_segment: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 173) as u8).collect();
    let expected_etag_first = multipart_etag(&[&initial_etag, &md5_hex(&append_payload)]);
    assert_eq!(append_etag, expected_etag_first);

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let _initial_body = aggregated.into_bytes();

    let append_resp_second = append_object_with_if_match(
        &client,
        &bucket,
        key,
        append_position + append_payload.len() as i64,
        &second_segment,
        Some(format!("\"{append_etag}\"")),
    )
    .await
    .expect("second segmented append must succeed");

    let expected_etag = multipart_etag(&[&initial_etag, &md5_hex(&append_payload), &md5_hex(&second_segment)]);
    assert_eq!(
        append_resp_second.e_tag().map(|v| v.trim_matches('"').to_string()),
        Some(expected_etag.clone())
    );

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();

    let mut expected = initial.clone();
    expected.extend_from_slice(&append_payload);
    expected.extend_from_slice(&second_segment);

    assert_eq!(body.as_ref(), expected.as_slice());

    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.content_length(), Some(expected.len() as i64));
    assert_eq!(head.e_tag().map(|v| v.trim_matches('"').to_string()), Some(expected_etag.clone()));

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_inline_object_rejects_failed_precondition() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-precond-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-if-match.txt";
    let initial = b"hello";
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;

    let append_payload = b" world";
    let err = append_object_with_if_match(
        &client,
        &bucket,
        key,
        initial.len() as i64,
        append_payload,
        Some("\"deadbeef\"".to_string()),
    )
    .await
    .expect_err("append with wrong If-Match must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 412);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial);

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_inline_object_honors_if_match() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-inline-if-match-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-inline-if-match-success.txt";
    let initial = b"inline";
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;
    let initial_etag = put_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("initial etag");

    let append_payload = b" payload";
    let resp = append_object_with_if_match(
        &client,
        &bucket,
        key,
        initial.len() as i64,
        append_payload,
        Some(format!("\"{initial_etag}\"")),
    )
    .await
    .expect("append with correct if-match should succeed");

    let combined: Vec<u8> = [initial.as_slice(), append_payload.as_slice()].concat();
    assert_eq!(resp.e_tag().map(|v| v.trim_matches('"').to_string()), Some(md5_hex(&combined)));

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), combined.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_inline_object_rejects_if_none_match_star() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-if-none-match-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-if-none.txt";
    let initial = b"hello";
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;

    let append_payload = b" world";
    let err =
        append_object_with_if_none_match(&client, &bucket, key, initial.len() as i64, append_payload, Some("*".to_string()))
            .await
            .expect_err("append with If-None-Match:* must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 412);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial);

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_inline_object_allows_zero_length_payload() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-inline-empty-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-inline-empty.txt";
    let initial = b"foobar";
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;
    let initial_etag = put_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("initial etag");

    let resp = append_object_with_if_match(&client, &bucket, key, initial.len() as i64, &[], Some(format!("\"{initial_etag}\"")))
        .await
        .expect("append with empty payload should succeed");

    assert_eq!(resp.e_tag().map(|v| v.trim_matches('"').to_string()), Some(initial_etag.clone()));

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial);

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn complete_append_consolidates_pending_segments() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-complete-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "complete-append.bin";
    let base_len = 256 * 1024;
    let base: Vec<u8> = (0..base_len).map(|i| (i % 251) as u8).collect();
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(base.clone()))
        .send()
        .await?;
    let base_etag = put_resp.e_tag().map(|v| v.trim_matches('"').to_string()).expect("base etag");

    let seg_a: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 199) as u8).collect();
    let seg_b: Vec<u8> = (0..(96 * 1024)).map(|i| (i % 173) as u8).collect();

    let append_a =
        append_object_with_if_match(&client, &bucket, key, base.len() as i64, &seg_a, Some(format!("\"{base_etag}\""))).await?;
    let etag_after_a = append_a
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("etag after first append");

    let append_b = append_object_with_if_match(
        &client,
        &bucket,
        key,
        (base.len() + seg_a.len()) as i64,
        &seg_b,
        Some(format!("\"{etag_after_a}\"")),
    )
    .await?;
    let etag_after_b = append_b
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("etag after second append");
    assert!(etag_after_b.contains('-'));

    let complete_resp = append_action(&client, &bucket, key, "complete", None).await?;
    let complete_etag = complete_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("complete etag");

    let mut expected = base.clone();
    expected.extend_from_slice(&seg_a);
    expected.extend_from_slice(&seg_b);

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.len(), expected.len());
    assert_eq!(final_body.as_ref(), expected.as_slice());
    assert_eq!(complete_etag, md5_hex(&expected));

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn abort_append_discards_pending_segments() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-abort-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "abort-append.bin";
    let base: Vec<u8> = (0..(512 * 1024)).map(|i| (i % 181) as u8).collect();
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(base.clone()))
        .send()
        .await?;
    let base_etag = put_resp.e_tag().map(|v| v.trim_matches('"').to_string()).expect("base etag");

    let seg_a: Vec<u8> = vec![0xAA; 64 * 1024];
    let seg_b: Vec<u8> = vec![0xBB; 96 * 1024];

    let append_a =
        append_object_with_if_match(&client, &bucket, key, base.len() as i64, &seg_a, Some(format!("\"{base_etag}\""))).await?;
    let etag_after_a = append_a
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("etag after first append");

    append_object_with_if_match(
        &client,
        &bucket,
        key,
        (base.len() + seg_a.len()) as i64,
        &seg_b,
        Some(format!("\"{etag_after_a}\"")),
    )
    .await?;

    let abort_resp = append_action(&client, &bucket, key, "abort", None).await?;
    let abort_etag = abort_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("abort etag");

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.len(), base.len());
    assert_eq!(final_body.as_ref(), base.as_slice());

    let retry_segment = vec![0xCC; 32 * 1024];
    let retry_resp = append_object_with_if_match(
        &client,
        &bucket,
        key,
        base.len() as i64,
        &retry_segment,
        Some(format!("\"{abort_etag}\"")),
    )
    .await?;
    let retry_etag = retry_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("retry etag");

    let mut expected_after_retry = base.clone();
    expected_after_retry.extend_from_slice(&retry_segment);
    let head_after_retry = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head_after_retry.content_length(), Some(expected_after_retry.len() as i64));
    assert_eq!(
        head_after_retry.e_tag().map(|v| v.trim_matches('"').to_string()),
        Some(retry_etag.clone())
    );

    let get_after_retry = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_bytes = get_after_retry.body.collect().await?.into_bytes();
    assert_eq!(final_bytes.as_ref(), expected_after_retry.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_segments_concurrency_then_complete() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-complete-concurrency-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "concurrent-complete.bin";
    let base: Vec<u8> = (0..(196 * 1024)).map(|i| (i % 233) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(base.clone()))
        .send()
        .await?;

    let seg_a: Vec<u8> = vec![0x11; 64 * 1024];
    let seg_b: Vec<u8> = vec![0x22; 48 * 1024];
    let seg_c: Vec<u8> = vec![0x33; 80 * 1024];

    let position = base.len() as i64;
    let client_a = client.clone();
    let client_b = client.clone();
    let bucket_a = bucket.clone();
    let bucket_b = bucket.clone();
    let key_string = key.to_string();
    let seg_a_clone = seg_a.clone();
    let seg_b_clone = seg_b.clone();

    let (res_a, res_b) = tokio::join!(
        async { append_object(&client_a, &bucket_a, &key_string, position, &seg_a_clone).await },
        async { append_object(&client_b, &bucket_b, &key_string, position, &seg_b_clone).await }
    );

    let (success_resp, failure_resp, winning_segment) = match (res_a, res_b) {
        (Ok(resp), Err(err)) => (resp, Some(err), seg_a.clone()),
        (Err(err), Ok(resp)) => (resp, Some(err), seg_b.clone()),
        _ => panic!("expected exactly one append success"),
    };

    if let Some(SdkError::ServiceError(service_err)) = failure_resp {
        assert_eq!(service_err.raw().status().as_u16(), 400);
    }

    let winning_etag = success_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("winning append etag");

    let mut expected = base.clone();
    expected.extend_from_slice(&winning_segment);

    append_object_with_if_match(&client, &bucket, key, expected.len() as i64, &seg_c, Some(format!("\"{winning_etag}\"")))
        .await?;

    expected.extend_from_slice(&seg_c);

    let complete_resp = append_action(&client, &bucket, key, "complete", None).await?;
    let final_etag = complete_resp
        .e_tag()
        .map(|v| v.trim_matches('"').to_string())
        .expect("final etag");

    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.content_length(), Some(expected.len() as i64));
    assert_eq!(head.e_tag().map(|v| v.trim_matches('"').to_string()), Some(final_etag.clone()));

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.as_ref(), expected.as_slice());
    assert_eq!(final_etag, md5_hex(&expected));

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_missing_object_returns_not_found() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-missing-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "missing-object.txt";
    let err = append_object(&client, &bucket, key, 0, b"data")
        .await
        .expect_err("append on missing object must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 404);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_segmented_object_rejects_wrong_position() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-seg-pos-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-seg-pos.bin";
    let initial: Vec<u8> = (0..(512 * 1024)).map(|i| (i % 211) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.clone()))
        .send()
        .await?;

    let err = append_object(&client, &bucket, key, (initial.len() as i64) + 1, b"abc")
        .await
        .expect_err("append with wrong position on segmented object must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 400);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_segmented_object_rejects_failed_precondition() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-seg-precond-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "append-seg-precond.bin";
    let initial: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 199) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.clone()))
        .send()
        .await?;

    let append_payload: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 173) as u8).collect();
    let err = append_object_with_if_match(
        &client,
        &bucket,
        key,
        initial.len() as i64,
        &append_payload,
        Some("\"ffffffffffffffffffffffffffffffff\"".to_string()),
    )
    .await
    .expect_err("append with wrong etag on segmented object must fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 412);
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let aggregated = get_resp.body.collect().await?;
    let body = aggregated.into_bytes();
    assert_eq!(body.as_ref(), initial.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_large_file_multi_segments() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-large-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "large-append.bin";

    // Create initial object with 1MB data
    let chunk_size = 1024 * 1024; // 1MB
    let initial_data: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial_data.clone()))
        .send()
        .await?;

    // Append multiple 1MB chunks to trigger segmented storage
    let mut expected_data = initial_data.clone();
    for i in 1..=5 {
        let append_chunk: Vec<u8> = (0..chunk_size).map(|j| ((j + i * 1000) % 256) as u8).collect();

        append_object(&client, &bucket, key, expected_data.len() as i64, &append_chunk)
            .await
            .expect("large file append should succeed");

        expected_data.extend_from_slice(&append_chunk);

        // Verify partial content after each append
        let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
        let body_bytes = get_resp.body.collect().await?.into_bytes();
        assert_eq!(body_bytes.len(), expected_data.len());

        // Verify first and last few bytes to ensure data integrity
        assert_eq!(&body_bytes[0..100], &expected_data[0..100]);
        let end_offset = expected_data.len() - 100;
        assert_eq!(&body_bytes[end_offset..], &expected_data[end_offset..]);
    }

    // Final verification of complete content
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.len(), expected_data.len());
    assert_eq!(final_body.as_ref(), expected_data.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_threshold_crossing_inline_to_segmented() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-threshold-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "threshold-test.dat";

    // Start with small inline data (should stay inline)
    let small_data = vec![0u8; 1024]; // 1KB
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(small_data.clone()))
        .send()
        .await?;

    let mut expected_data = small_data;

    // Make several small appends to gradually grow the object
    for i in 1..=10 {
        let append_data = vec![i as u8; 2048]; // 2KB each

        append_object(&client, &bucket, key, expected_data.len() as i64, &append_data)
            .await
            .expect("threshold crossing append should succeed");

        expected_data.extend_from_slice(&append_data);

        // Wait a bit between appends to allow background spill processing
        sleep(Duration::from_millis(200)).await;
    }

    // Add one large append that definitely triggers segmented mode
    let large_append = vec![255u8; 512 * 1024]; // 512KB
    append_object(&client, &bucket, key, expected_data.len() as i64, &large_append)
        .await
        .expect("large append triggering segmentation should succeed");

    expected_data.extend_from_slice(&large_append);

    // Allow time for any background spill operations to complete
    sleep(Duration::from_secs(2)).await;

    // Verify final content integrity
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.len(), expected_data.len());
    assert_eq!(final_body.as_ref(), expected_data.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_concurrent_operations_with_epoch() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-concurrent-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "concurrent-test.txt";
    let initial = b"base";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial.to_vec()))
        .send()
        .await?;

    // Get initial object to obtain ETag for conditional append
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let initial_etag = get_resp.e_tag().unwrap().to_string();

    // First append with correct ETag should succeed
    let first_append = b" data1";
    let resp1 =
        append_object_with_if_match(&client, &bucket, key, initial.len() as i64, first_append, Some(initial_etag.clone()))
            .await
            .expect("first conditional append should succeed");

    let new_etag = resp1.e_tag().unwrap().to_string();

    // Second append with old ETag should fail (simulating concurrent modification)
    let second_append = b" data2";
    let err = append_object_with_if_match(
        &client,
        &bucket,
        key,
        (initial.len() + first_append.len()) as i64,
        second_append,
        Some(initial_etag), // Using old ETag should fail
    )
    .await
    .expect_err("append with stale etag should fail");

    match err {
        SdkError::ServiceError(service_err) => {
            assert_eq!(service_err.raw().status().as_u16(), 412); // Precondition Failed
        }
        other => panic!("unexpected error variant: {other:?}"),
    }

    // Third append with correct new ETag should succeed
    append_object_with_if_match(
        &client,
        &bucket,
        key,
        (initial.len() + first_append.len()) as i64,
        second_append,
        Some(new_etag),
    )
    .await
    .expect("append with correct etag should succeed");

    // Verify final content
    let expected: Vec<u8> = [initial.as_slice(), first_append.as_slice(), second_append.as_slice()].concat();
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let final_body = get_resp.body.collect().await?.into_bytes();
    assert_eq!(final_body.as_ref(), expected.as_slice());

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn append_range_requests_across_segments() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-range-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "range-test.dat";

    // Create base object with known pattern
    let base_size = 10000;
    let base_data: Vec<u8> = (0..base_size).map(|i| (i % 256) as u8).collect();

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(base_data.clone()))
        .send()
        .await?;

    // Append multiple segments with different patterns
    let mut expected_data = base_data;
    for segment in 1..=3 {
        let segment_size = 5000 + segment * 1000; // Variable segment sizes
        let segment_data: Vec<u8> = (0..segment_size).map(|i| ((i + segment * 100) % 256) as u8).collect();

        append_object(&client, &bucket, key, expected_data.len() as i64, &segment_data)
            .await
            .expect("segment append should succeed");

        expected_data.extend_from_slice(&segment_data);
    }

    sleep(Duration::from_millis(500)).await; // Allow background processing

    // Test various range requests that cross segment boundaries
    let test_ranges = [
        (0, 999),                                              // Beginning of base segment
        (9000, 11000),                                         // Across base and first append
        (15000, 20000),                                        // Middle of appended data
        (expected_data.len() - 1000, expected_data.len() - 1), // End of data
    ];

    for (start, end) in test_ranges {
        let range_header = format!("bytes={}-{}", start, end);
        let range_resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .range(&range_header)
            .send()
            .await?;

        let content_range = range_resp.content_range().unwrap().to_string();
        let range_body = range_resp.body.collect().await?.into_bytes();
        let expected_range = &expected_data[start..=end];

        assert_eq!(range_body.len(), expected_range.len());
        assert_eq!(range_body.as_ref(), expected_range);
        assert_eq!(content_range, format!("bytes {}-{}/{}", start, end, expected_data.len()));
    }

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}
