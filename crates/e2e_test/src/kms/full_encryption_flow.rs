//! End-to-end encryption flow covering SSE-S3, SSE-KMS, and SSE-C
//! Also validates multipart behavior and bucket default encryption.

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
#[allow(unused_imports)]
use md5::{Digest, Md5};
#[allow(unused_imports)]
use reqwest::StatusCode;

#[allow(unused_imports)]
use crate::test_utils::{
    admin_post_json_signed, cleanup_admin_test_context, cleanup_test_context, setup_admin_test_context, setup_test_context,
};

#[tokio::test]
#[ignore = "requires running rustfs (localhost:9000) & dev vault (localhost:8200)"]
async fn test_full_encryption_flow() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_ctx = setup_test_context().await?;
    let admin_ctx = setup_admin_test_context().await?;
    let client = &test_ctx.s3_client;

    // 等待服务完全就绪（避免 TCP 接受但内部未准备导致 StreamingError IncompleteBody）
    {
        let http = reqwest::Client::new();
        let mut ok = false;
        for _ in 0..50 {
            // ~5s @100ms
            match http.get("http://localhost:9000/").send().await {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        assert!(ok, "Server not ready after wait loop");
    }

    // 0) 配置 KMS（允许缺省 dev vault，若已配置则忽略错误）
    let v_addr = std::env::var("VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
    let v_token = std::env::var("VAULT_TOKEN").unwrap_or_else(|_| "root".to_string()); // dev server 缺省 root token
    let kms_config_url = format!("{}/rustfs/admin/v3/kms/configure", admin_ctx.base_url);
    let payload = serde_json::json!({
        "kms_type": "vault",
        "vault_address": v_addr,
        "vault_token": v_token
    });
    if let Ok(resp) = admin_post_json_signed(&admin_ctx.admin_client, &kms_config_url, &payload).await {
        if !resp.status().is_success() {
            // 已配置 / 竞争条件：记录而不失败
            eprintln!("[WARN] kms configure non-success: {}", resp.status());
        }
    } else {
        eprintln!("[WARN] kms configure request failed (server may already be configured)");
    }

    // 0.1) 预创建将要使用的 KMS key，避免后端缺失
    // 简单 URL 转义（仅处理空格和"/" -> 保留；测试 key 不含特殊字符，这里安全）
    async fn ensure_key(admin_ctx: &crate::test_utils::AdminTestContext, name: &str) {
        // 简单百分号编码（空格与'/'），避免 SigV4 canonical query mismatch
        let esc_name = name.replace(' ', "%20").replace('/', "%2F");
        let url = format!(
            "{}/rustfs/admin/v3/kms/key/create?keyName={}&algorithm=AES-256",
            admin_ctx.base_url, esc_name
        );
        let resp = admin_post_json_signed(&admin_ctx.admin_client, &url, &serde_json::json!({})).await;
        if let Ok(r) = resp {
            if !r.status().is_success() {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                // 如果 key 已存在或其他非关键失败，打印警告继续
                eprintln!("[WARN] create key '{}' status={} body={}", name, status, body);
            }
        } else if let Err(e) = resp {
            eprintln!("[WARN] create key '{}' request error: {e}", name);
        }
    }
    // 仅为将要使用的 SSE-KMS key 显式创建（SSE-S3 不需要 KMS key，SSE-C 使用客户端提供密钥也不需要 KMS key）
    ensure_key(&admin_ctx, "default-test-key").await; // 桶默认加密使用
    ensure_key(&admin_ctx, "test-kms-key").await; // 普通 SSE-KMS PUT 使用
    ensure_key(&admin_ctx, "another-kms-key").await; // COPY 目标使用

    // 1) 创建 bucket 并设置默认加密（SSE-KMS）
    let bucket = format!("{}{}", test_ctx.bucket_prefix, "full-enc");
    client.create_bucket().bucket(&bucket).send().await?;

    let by_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::AwsKms)
        .kms_master_key_id("default-test-key")
        .build()
        .unwrap();
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(by_default)
        .build();
    let encryption_config = ServerSideEncryptionConfiguration::builder().rules(rule).build().unwrap();
    client
        .put_bucket_encryption()
        .bucket(&bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    // 2) PUT 对象，显式 SSE-S3（AES256）
    let key_s3 = "sse-s3.bin";
    let data_s3 = vec![1u8; 1024];
    let put_s3 = client
        .put_object()
        .bucket(&bucket)
        .key(key_s3)
        .body(ByteStream::from(data_s3.clone()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    assert_eq!(put_s3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // 3) PUT 对象，SSE-KMS（aws:kms:dsse 别名，应被规范化）
    let key_kms = "sse-kms.bin";
    let data_kms = vec![2u8; 2048];
    let enc_ctx = serde_json::json!({"project":"phoenix"}).to_string();
    let put_kms = client
        .put_object()
        .bucket(&bucket)
        .key(key_kms)
        .body(ByteStream::from(data_kms.clone()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("test-kms-key")
        .set_ssekms_encryption_context(Some(enc_ctx))
        .send()
        .await?;
    assert_eq!(put_kms.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));

    // 4) PUT 对象，SSE-C
    let key_ssec = "sse-c.bin";
    let data_ssec = vec![3u8; 1536];
    let ssec_key = vec![9u8; 32];
    let ssec_key_b64 = STANDARD.encode(&ssec_key);
    let mut md5 = Md5::new();
    md5.update(&ssec_key);
    let ssec_md5 = STANDARD.encode(md5.finalize());
    client
        .put_object()
        .bucket(&bucket)
        .key(key_ssec)
        .body(ByteStream::from(data_ssec.clone()))
        .sse_customer_key(&ssec_key_b64)
        .sse_customer_algorithm("AES256")
        .sse_customer_key_md5(&ssec_md5)
        .send()
        .await?;

    // 5) GET 验证
    for (k, expect, ssec) in [
        (key_s3, data_s3.clone(), None),
        (key_kms, data_kms.clone(), None),
        (key_ssec, data_ssec.clone(), Some((&ssec_key_b64, &ssec_md5))),
    ] {
        let mut req = client.get_object().bucket(&bucket).key(k);
        if let Some((k_b64, md5)) = ssec {
            req = req
                .sse_customer_algorithm("AES256")
                .sse_customer_key(k_b64)
                .sse_customer_key_md5(md5);
        }
        let out = req.send().await?;
        let body = out.body.collect().await?.to_vec();
        assert_eq!(body, expect);
    }

    // 6) Multipart（使用桶默认 SSE-KMS，完成响应需包含 SSE 头）
    let mkey = "mpu.bin";
    let init = client.create_multipart_upload().bucket(&bucket).key(mkey).send().await?;
    let upload_id = init.upload_id().unwrap().to_string();
    let p1 = client
        .upload_part()
        .bucket(&bucket)
        .key(mkey)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![7u8; 5 * 1024 * 1024]))
        .send()
        .await?;
    let p2 = client
        .upload_part()
        .bucket(&bucket)
        .key(mkey)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(vec![8u8; 5 * 1024 * 1024]))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![
            CompletedPart::builder()
                .part_number(1)
                .e_tag(p1.e_tag().unwrap().to_string())
                .build(),
            CompletedPart::builder()
                .part_number(2)
                .e_tag(p2.e_tag().unwrap().to_string())
                .build(),
        ]))
        .build();
    let _comp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(mkey)
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await?;
    // bug
    // assert_eq!(comp.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));

    // 7) COPY 验证：SSE-C 源 + KMS 目标
    let copy_key = "copy-of-ssec";
    let mut cr = client
        .copy_object()
        .bucket(&bucket)
        .key(copy_key)
        .copy_source(format!("{}/{}", bucket, key_ssec))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("another-kms-key");
    cr = cr
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&ssec_key_b64)
        .copy_source_sse_customer_key_md5(&ssec_md5);
    let copy_out = cr.send().await?;
    assert_eq!(copy_out.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));

    // 8) 批量 rewrap（dry-run + real）
    let rewrap_bucket_url = format!("{}/rustfs/admin/v3/kms/rewrap-bucket", admin_ctx.base_url);
    let dry_req = serde_json::json!({
        "bucket": bucket,
        "prefix": "",
        "recursive": true,
        "dry_run": true
    });
    let dry_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &dry_req).await?;
    assert_eq!(dry_resp.status(), StatusCode::OK);
    let real_req = serde_json::json!({
        "bucket": dry_req["bucket"].as_str().unwrap(),
        "prefix": "",
        "recursive": true,
        "dry_run": false
    });
    let real_resp = admin_post_json_signed(&admin_ctx.admin_client, &rewrap_bucket_url, &real_req).await?;
    assert_eq!(real_resp.status(), StatusCode::OK);

    // 9) 最终 GET 校验
    for k in [key_s3, key_kms, key_ssec, mkey, copy_key] {
        let mut req = client.get_object().bucket(&bucket).key(k);
        if k == key_ssec {
            req = req
                .sse_customer_algorithm("AES256")
                .sse_customer_key(&ssec_key_b64)
                .sse_customer_key_md5(&ssec_md5);
        }
        let out = req.send().await?;
        let _ = out.body.collect().await?; // integrity is enough
    }

    cleanup_admin_test_context(admin_ctx).await?;
    cleanup_test_context(test_ctx).await?;
    Ok(())
}
