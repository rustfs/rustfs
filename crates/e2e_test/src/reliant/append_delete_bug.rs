#![cfg(test)]

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use http::HeaderValue;
use serial_test::serial;
use std::error::Error;
use std::path::PathBuf;
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
        })
        .send()
        .await
}

/// 检查磁盘上是否存在 append segments 目录
async fn check_append_segments_exist(bucket: &str, object: &str, data_dirs: &[PathBuf]) -> bool {
    for data_dir in data_dirs {
        let bucket_path = data_dir.join(bucket);
        let object_path = bucket_path.join(object);
        let append_path = object_path.join("append");

        if tokio::fs::metadata(&append_path).await.is_ok() {
            // 检查是否有子目录
            if let Ok(mut entries) = tokio::fs::read_dir(&append_path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if entry.file_type().await.map(|ft| ft.is_dir()).unwrap_or(false) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

#[tokio::test]
#[serial]
async fn delete_object_with_pending_append_segments_should_cleanup() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-delete-bug-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "object-with-pending-segments.bin";

    // 创建一个大对象以触发 segmented 模式
    let initial_size = 512 * 1024; // 512KB
    let initial_data: Vec<u8> = (0..initial_size).map(|i| (i % 251) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial_data.clone()))
        .send()
        .await?;

    // 执行多次 append 操作创建 pending segments
    let append1: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 197) as u8).collect();
    append_object(&client, &bucket, key, initial_data.len() as i64, &append1)
        .await
        .expect("first append should succeed");

    let append2: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 173) as u8).collect();
    append_object(
        &client,
        &bucket,
        key,
        (initial_data.len() + append1.len()) as i64,
        &append2,
    )
    .await
    .expect("second append should succeed");

    // 等待一下确保 append 操作完成
    sleep(Duration::from_millis(500)).await;

    // 验证对象存在且包含所有数据
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let body = get_resp.body.collect().await?.into_bytes();
    let mut expected = initial_data.clone();
    expected.extend_from_slice(&append1);
    expected.extend_from_slice(&append2);
    assert_eq!(body.len(), expected.len());

    // 获取数据目录路径
    let data_dirs = env.get_data_directories()?;

    // 验证 append segments 目录存在（证明确实创建了 pending segments）
    let segments_exist_before = check_append_segments_exist(&bucket, key, &data_dirs).await;
    assert!(
        segments_exist_before,
        "Append segments directory should exist before deletion"
    );

    // 直接删除对象（不执行 complete 或 abort）
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    // 等待一下确保删除操作完成
    sleep(Duration::from_millis(500)).await;

    // 验证对象已被删除
    let head_result = client.head_object().bucket(&bucket).key(key).send().await;
    assert!(head_result.is_err(), "Object should be deleted");

    // 关键验证：append segments 目录应该被清理
    let segments_exist_after = check_append_segments_exist(&bucket, key, &data_dirs).await;
    assert!(
        !segments_exist_after,
        "Append segments directory should be cleaned up after object deletion. \
         This is the bug: pending segments are not cleaned up when deleting objects."
    );

    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn delete_object_after_abort_should_not_leave_segments() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("append-delete-after-abort-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "object-abort-then-delete.bin";

    // 创建对象
    let initial_size = 256 * 1024; // 256KB
    let initial_data: Vec<u8> = (0..initial_size).map(|i| (i % 211) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial_data.clone()))
        .send()
        .await?;

    // 执行 append
    let append_data: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 193) as u8).collect();
    append_object(&client, &bucket, key, initial_data.len() as i64, &append_data)
        .await
        .expect("append should succeed");

    // 执行 abort
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b""))
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-append-action", HeaderValue::from_static("abort"));
        })
        .send()
        .await?;

    sleep(Duration::from_millis(500)).await;

    // 获取数据目录
    let data_dirs = env.get_data_directories()?;

    // abort 之后 segments 应该已经被清理
    let segments_after_abort = check_append_segments_exist(&bucket, key, &data_dirs).await;
    assert!(
        !segments_after_abort,
        "Append segments should be cleaned up after abort"
    );

    // 删除对象
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    sleep(Duration::from_millis(500)).await;

    // 验证没有残留的 append 目录
    let segments_after_delete = check_append_segments_exist(&bucket, key, &data_dirs).await;
    assert!(
        !segments_after_delete,
        "No append segments should remain after deletion"
    );

    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

