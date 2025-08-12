//! Rewrap tests for SSE-KMS objects
//! - Batch rewrap by bucket/prefix with dry_run and actual update
//! - Single-object case is covered via a narrow-prefix batch rewrap

#[allow(unused_imports)]
use super::{cleanup_test_context, setup_test_context};
#[allow(unused_imports)]
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};
#[allow(unused_imports)]
use base64::{Engine, engine::general_purpose::STANDARD};
#[allow(unused_imports)]
use reqwest::StatusCode;

#[allow(unused_imports)]
use crate::test_utils::{admin_post_json_signed, cleanup_admin_test_context, setup_admin_test_context};

#[tokio::test]
#[ignore = "requires running RustFS server and admin API at localhost:9000"]
async fn test_single_ciphertext_rewrap() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 说明：在密封元数据模型中不再公开 wrapped DEK，单密文 rewrap 不从对象头部读取。
    // 这里用“窄前缀”的批量 rewrap 来覆盖单对象场景。
    let test_context = setup_test_context().await?;
    let admin_ctx = setup_admin_test_context().await?;
    let client = &test_context.s3_client;

    let bucket = format!("{}{}", test_context.bucket_prefix, "rewrap-single-bucket");
    let key = "obj";

    // 1) 创建 bucket
    client.create_bucket().bucket(&bucket).send().await?;

    // 2) 预创建所需 SSE-KMS key（忽略已存在错误）
    let create_url = format!(
        "{}/rustfs/admin/v3/kms/key/create?keyName=test-kms-key&algorithm=AES-256",
        admin_ctx.base_url
    );
    let _ = admin_post_json_signed(&admin_ctx.admin_client, &create_url, &serde_json::json!({})).await;

    // 3) 上传一个 SSE-KMS 对象，附带简单 context
    let data = b"hello rewrap single";
    let enc_ctx_json = serde_json::json!({"team":"alpha"}).to_string();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("test-kms-key")
        .set_ssekms_encryption_context(Some(enc_ctx_json.clone()))
        .send()
        .await?;

    // 4) 先 dry-run 看看会处理几个
    let rewrap_bucket_url = format!("{}/rustfs/admin/v3/kms/rewrap-bucket", admin_ctx.base_url);
    let dry_req = serde_json::json!({
        "bucket": bucket,
        "prefix": key,
        "recursive": false,
        "dry_run": true
    });
    let dry_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &dry_req).await?;
    assert_eq!(dry_resp.status(), StatusCode::OK);

    // 5) 真实执行批量 rewrap（只会命中该对象）
    let real_req = serde_json::json!({
        "bucket": dry_req["bucket"].as_str().unwrap_or(""),
        "prefix": key,
        "recursive": false,
        "dry_run": false
    });
    let real_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &real_req).await?;
    assert_eq!(real_resp.status(), StatusCode::OK);

    // 6) 下载验证数据一致
    let got = client.get_object().bucket(&bucket).key(key).send().await?;
    let body = got.body.collect().await?.to_vec();
    assert_eq!(body, data);

    cleanup_admin_test_context(admin_ctx).await?;
    cleanup_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server and admin API at localhost:9000"]
async fn test_batch_rewrap_bucket() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_test_context().await?;
    let admin_ctx = setup_admin_test_context().await?;
    let client = &test_context.s3_client;

    let bucket = format!("{}{}", test_context.bucket_prefix, "rewrap-batch-bucket");

    // 1) 创建 bucket
    client.create_bucket().bucket(&bucket).send().await?;

    // 2) 预创建 SSE-KMS key
    let create_url = format!(
        "{}/rustfs/admin/v3/kms/key/create?keyName=test-kms-key&algorithm=AES-256",
        admin_ctx.base_url
    );
    let _ = admin_post_json_signed(&admin_ctx.admin_client, &create_url, &serde_json::json!({})).await;

    // 3) 上传数个 SSE-KMS 对象（带不同 context 字段）
    for i in 0..3 {
        let key = format!("pfx/obj-{}", i);
        let data = format!("hello-{}", i).into_bytes();
        let enc_ctx_json = serde_json::json!({"team":"beta","index": i}).to_string();
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id("test-kms-key")
            .set_ssekms_encryption_context(Some(enc_ctx_json))
            .send()
            .await?;
    }

    // 4) 不再读取公开的 wrapped DEK（密封元数据不对外暴露）

    // 5) dry_run 批量 rewrap
    let dry_req = serde_json::json!({
        "bucket": bucket,
        "prefix": "pfx/",
        "recursive": true,
        "dry_run": true
    });
    let rewrap_bucket_url = format!("{}/rustfs/admin/v3/kms/rewrap-bucket", admin_ctx.base_url);
    let dry_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &dry_req).await?;
    assert_eq!(dry_resp.status(), StatusCode::OK);
    let dry_json: serde_json::Value = dry_resp.json().await?;
    let processed = dry_json["processed"].as_u64().unwrap_or(0);
    let rewrapped = dry_json["rewrapped"].as_u64().unwrap_or(0);
    assert!(processed >= 3);
    assert!(rewrapped >= 3); // 都是候选

    // 6) 真正执行批量 rewrap
    let real_req = serde_json::json!({
        "bucket": dry_json["bucket"].as_str().unwrap_or(""),
        "prefix": "pfx/",
        "recursive": true,
        "dry_run": false
    });
    let real_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &real_req).await?;
    assert_eq!(real_resp.status(), StatusCode::OK);
    let real_json: serde_json::Value = real_resp.json().await?;
    let rewrapped2 = real_json["rewrapped"].as_u64().unwrap_or(0);
    assert!(rewrapped2 >= 3);

    // 7) 逐个下载验证（内容应保持不变）
    for i in 0..3 {
        let key = format!("pfx/obj-{}", i);
        let expect = format!("hello-{}", i).into_bytes();
        let got = client.get_object().bucket(&bucket).key(&key).send().await?;
        let body = got.body.collect().await?.to_vec();
        assert_eq!(body, expect);
    }

    cleanup_admin_test_context(admin_ctx).await?;
    cleanup_test_context(test_context).await?;
    Ok(())
}
