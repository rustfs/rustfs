//! Rewrap tests for SSE-KMS objects
//! - Single ciphertext rewrap via admin API
//! - Batch rewrap by bucket/prefix with dry_run and actual update

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
    let test_context = setup_test_context().await?;
    let admin_ctx = setup_admin_test_context().await?;
    let client = &test_context.s3_client;

    let bucket = format!("{}{}", test_context.bucket_prefix, "rewrap-single-bucket");
    let key = "obj";

    // 1) 创建 bucket
    client.create_bucket().bucket(&bucket).send().await?;

    // 2) 上传一个 SSE-KMS 对象，附带简单 context
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

    // 3) 读取对象 metadata，拿到 wrapped DEK（旧值）
    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    let user_md = head.metadata().cloned().unwrap_or_default();
    let old_wrapped_b64 = user_md
        .get("x-amz-server-side-encryption-key")
        .cloned()
        .ok_or("missing wrapped key in metadata")?;

    // 4) 调用 admin rewrap 接口
    let payload = serde_json::json!({
        "ciphertext_b64": old_wrapped_b64,
        "context": {"bucket": bucket, "key": key, "team": "alpha"}
    });

    let rewrap_url = format!("{}/rustfs/admin/v3/kms/rewrap", admin_ctx.base_url);
    let resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_url, &payload).await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await?;
    let new_b64 = v["ciphertext_b64"].as_str().unwrap_or("");
    assert!(!new_b64.is_empty());

    // 5) 实际更新对象元数据：调用批量重包裹端点（只包含当前对象）
    let real_req = serde_json::json!({
        "bucket": bucket,
        "prefix": "",
        "recursive": true,
        "dry_run": false
    });
    let rewrap_bucket_url = format!("{}/rustfs/admin/v3/kms/rewrap-bucket", admin_ctx.base_url);
    let real_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &real_req).await?;
    assert_eq!(real_resp.status(), StatusCode::OK);

    // 6) 再次 HEAD，确认 wrapped DEK 发生变化
    let head2 = client.head_object().bucket(&bucket).key(key).send().await?;
    let user_md2 = head2.metadata().cloned().unwrap_or_default();
    let new_wrapped_b64 = user_md2
        .get("x-amz-server-side-encryption-key")
        .cloned()
        .ok_or("missing wrapped key after rewrap")?;
    assert_ne!(new_wrapped_b64, old_wrapped_b64);

    // 7) 验证 rewrap 后仍可正常下载
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

    // 2) 上传数个 SSE-KMS 对象（带不同 context 字段）
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

    // 3) 记录各对象当前 wrapped DEK
    use std::collections::HashMap;
    let mut before_keys: HashMap<String, String> = HashMap::new();
    for i in 0..3 {
        let k = format!("pfx/obj-{}", i);
        let h = client.head_object().bucket(&bucket).key(&k).send().await?;
        let md = h.metadata().cloned().unwrap_or_default();
        let w = md.get("x-amz-server-side-encryption-key").cloned().unwrap_or_default();
        before_keys.insert(k, w);
    }

    // 4) dry_run 批量 rewrap
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

    // 5) 真正执行批量 rewrap
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

    // 6) 逐个下载验证，并检查 wrapped DEK 确实变化
    for i in 0..3 {
        let key = format!("pfx/obj-{}", i);
        let expect = format!("hello-{}", i).into_bytes();
        let got = client.get_object().bucket(&bucket).key(&key).send().await?;
        let body = got.body.collect().await?.to_vec();
        assert_eq!(body, expect);

        let h2 = client.head_object().bucket(&bucket).key(&key).send().await?;
        let md2 = h2.metadata().cloned().unwrap_or_default();
        let new_w = md2.get("x-amz-server-side-encryption-key").cloned().unwrap_or_default();
        let old_w = before_keys.get(&key).cloned().unwrap_or_default();
        assert_ne!(new_w, old_w);
    }

    cleanup_admin_test_context(admin_ctx).await?;
    cleanup_test_context(test_context).await?;
    Ok(())
}
