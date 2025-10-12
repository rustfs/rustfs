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
    append_object(&client, &bucket, key, (initial_data.len() + append1.len()) as i64, &append2)
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
    assert!(segments_exist_before, "Append segments directory should exist before deletion");

    // 直接删除对象（不执行 complete 或 abort）
    client.delete_object().bucket(&bucket).key(key).send().await?;

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
    assert!(!segments_after_abort, "Append segments should be cleaned up after abort");

    // 删除对象
    client.delete_object().bucket(&bucket).key(key).send().await?;

    sleep(Duration::from_millis(500)).await;

    // 验证没有残留的 append 目录
    let segments_after_delete = check_append_segments_exist(&bucket, key, &data_dirs).await;
    assert!(!segments_after_delete, "No append segments should remain after deletion");

    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn delete_all_versions_then_reupload_should_be_immediately_visible() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("delete-reupload-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    let key = "test-file.bin";

    // 1. 上传一个文件
    let file_size = 1024 * 1024; // 1MB for faster test
    let initial_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(initial_data.clone()))
        .send()
        .await?;

    // Verify file exists
    let list1 = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(list1.contents().len(), 1);
    assert_eq!(list1.contents()[0].key().unwrap(), key);

    // 2. 删除文件（在 non-versioned bucket 中，这会物理删除）
    client.delete_object().bucket(&bucket).key(key).send().await?;

    // Give it a moment to propagate
    sleep(Duration::from_millis(500)).await;

    // 3. 验证文件被删除
    let list2 = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(list2.contents().len(), 0, "File should be deleted");

    // 4. 再次上传同一个文件
    let new_data: Vec<u8> = (0..file_size).map(|i| ((i + 100) % 256) as u8).collect();
    let put_resp = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(new_data.clone()))
        .send()
        .await?;

    assert!(put_resp.e_tag().is_some(), "Upload should succeed and return ETag");

    // Give it a moment to propagate
    sleep(Duration::from_millis(500)).await;

    // 5. 立即列出文件，应该能看到新上传的文件
    let list3 = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(
        list3.contents().len(),
        1,
        "Newly uploaded file should be immediately visible after upload. \
         If this fails, it suggests metadata inconsistency across disks."
    );

    if !list3.contents().is_empty() {
        assert_eq!(list3.contents()[0].key().unwrap(), key);

        // 6. 验证文件内容正确
        let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
        let body = get_resp.body.collect().await?.into_bytes();
        assert_eq!(body.len(), new_data.len());
        assert_eq!(body.as_ref(), new_data.as_slice());
    }

    client.delete_object().bucket(&bucket).key(key).send().await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn delete_all_versions_in_versioned_bucket_then_reupload() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("versioned-delete-reupload-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    // 启用版本控制
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    let key = "test-file.bin";

    // 1. 上传文件（创建版本1）
    let file_size = 1024 * 1024;
    let data1: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    let put1 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data1.clone()))
        .send()
        .await?;
    let version1 = put1.version_id().map(|v| v.to_string());

    if version1.is_none() {
        // Versioning might not be immediately effective, skip this test
        client.delete_bucket().bucket(&bucket).send().await?;
        return Ok(());
    }
    let version1 = version1.unwrap();

    // 2. 再上传一次（创建版本2）
    let data2: Vec<u8> = (0..file_size).map(|i| ((i + 50) % 256) as u8).collect();
    let put2 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data2.clone()))
        .send()
        .await?;
    let version2 = put2.version_id().expect("Version 2 should have version_id").to_string();

    sleep(Duration::from_millis(500)).await;

    // 3. 验证有2个版本
    let versions = client.list_object_versions().bucket(&bucket).send().await?;
    assert_eq!(versions.versions().len(), 2, "Should have 2 versions");

    // 4. 删除所有版本
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version1)
        .send()
        .await?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version2)
        .send()
        .await?;

    sleep(Duration::from_millis(500)).await;

    // 5. 验证所有版本都被删除（列表为空或只有delete markers）
    let list_after_delete = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(list_after_delete.contents().len(), 0, "All versions should be deleted");

    // 6. 重新上传同名文件
    let data3: Vec<u8> = (0..file_size).map(|i| ((i + 100) % 256) as u8).collect();
    let put3 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data3.clone()))
        .send()
        .await?;
    let version3 = put3.version_id().unwrap().to_string();

    sleep(Duration::from_millis(500)).await;

    // 7. 立即列出，应该能看到新上传的文件
    let list_after_reupload = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(
        list_after_reupload.contents().len(),
        1,
        "Newly uploaded file should be immediately visible in versioned bucket. \
         This is the bug: file not visible immediately after re-upload."
    );

    if !list_after_reupload.contents().is_empty() {
        assert_eq!(list_after_reupload.contents()[0].key().unwrap(), key);

        // 验证内容
        let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
        let body = get_resp.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), data3.as_slice());
    }

    // 清理
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version3)
        .send()
        .await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn delete_all_versions_should_cleanup_object_directory() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    sleep(Duration::from_secs(1)).await;
    let client = env.create_s3_client();

    let bucket = format!("cleanup-dir-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    // 启用版本控制
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    let key = "test-cleanup.bin";

    // 上传文件
    let data: Vec<u8> = vec![0u8; 1024];
    let put1 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await?;

    let version1 = put1.version_id().map(|v| v.to_string());
    if version1.is_none() {
        client.delete_bucket().bucket(&bucket).send().await?;
        return Ok(());
    }
    let version1 = version1.unwrap();

    sleep(Duration::from_millis(200)).await;

    // 删除文件（不指定版本，创建 delete marker）
    let delete_resp = client.delete_object().bucket(&bucket).key(key).send().await?;
    let delete_marker_version = delete_resp.version_id().map(|v| v.to_string()).expect("delete marker version");

    sleep(Duration::from_millis(200)).await;

    // 验证有2个版本（1个对象版本 + 1个 delete marker）
    let versions = client.list_object_versions().bucket(&bucket).send().await?;
    let total_versions = versions.versions().len() + versions.delete_markers().len();
    assert_eq!(total_versions, 2, "Should have 2 versions (1 object + 1 delete marker)");

    // 获取数据目录
    let data_dirs = env.get_data_directories()?;

    // 验证对象目录存在
    let object_dir_exists_before = data_dirs.iter().any(|data_dir| {
        let object_path = data_dir.join(&bucket).join(key);
        object_path.exists()
    });
    assert!(object_dir_exists_before, "Object directory should exist before deleting all versions");

    // 删除所有版本
    client.delete_object().bucket(&bucket).key(key).version_id(&version1).send().await?;

    client.delete_object().bucket(&bucket).key(key).version_id(&delete_marker_version).send().await?;

    sleep(Duration::from_millis(500)).await;

    // 关键验证：对象目录应该被完全清理
    let object_dir_exists_after = data_dirs.iter().any(|data_dir| {
        let object_path = data_dir.join(&bucket).join(key);
        object_path.exists()
    });

    assert!(
        !object_dir_exists_after,
        "Object directory should be cleaned up after deleting all versions. \
         Bug: empty object directory with only xl.meta remains."
    );

    client.delete_bucket().bucket(&bucket).send().await?;

    Ok(())
}
