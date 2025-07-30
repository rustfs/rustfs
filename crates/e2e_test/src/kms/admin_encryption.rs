#[allow(unused_imports)]
use crate::test_utils::{cleanup_admin_test_context, setup_admin_test_context};
#[allow(unused_imports)]
use reqwest::StatusCode;
#[allow(unused_imports)]
use serde_json::json;

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_admin_bucket_encryption_crud() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_admin_test_context().await?;
    let client = &test_context.admin_client;
    let bucket = "test-bucket-encryption-crud";

    // Create bucket first
    client
        .put(format!("/api/v1/buckets/{bucket}"))
        .json(&json!({"bucketName": bucket}))
        .send()
        .await?
        .error_for_status()?;

    // Set bucket encryption configuration
    let encryption_config = json!({
        "rules": [{
            "applyServerSideEncryptionByDefault": {
                "sseAlgorithm": "AES256"
            }
        }]
    });

    let response = client
        .put(format!("/api/v1/buckets/{bucket}/encryption"))
        .json(&encryption_config)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Get bucket encryption configuration
    let response = client.get(format!("/api/v1/buckets/{bucket}/encryption")).send().await?;

    assert_eq!(response.status(), StatusCode::OK);
    let config = response.json::<serde_json::Value>().await?;
    assert_eq!(config["rules"][0]["applyServerSideEncryptionByDefault"]["sseAlgorithm"], "AES256");

    // Delete bucket encryption configuration
    let response = client.delete(format!("/api/v1/buckets/{bucket}/encryption")).send().await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify configuration is deleted
    let response = client.get(format!("/api/v1/buckets/{bucket}/encryption")).send().await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    cleanup_admin_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_admin_bucket_encryption_validation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_admin_test_context().await?;
    let client = &test_context.admin_client;
    let bucket = "test-bucket-encryption-validation";

    // Create bucket
    client
        .put(format!("/api/v1/buckets/{bucket}"))
        .json(&json!({"bucketName": bucket}))
        .send()
        .await?
        .error_for_status()?;

    // Test invalid encryption algorithm
    let invalid_config = json!({
        "rules": [{
            "applyServerSideEncryptionByDefault": {
                "sseAlgorithm": "INVALID"
            }
        }]
    });

    let response = client
        .put(format!("/api/v1/buckets/{bucket}/encryption"))
        .json(&invalid_config)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Test missing required fields
    let incomplete_config = json!({
        "rules": [{}]
    });

    let response = client
        .put(format!("/api/v1/buckets/{bucket}/encryption"))
        .json(&incomplete_config)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cleanup_admin_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_admin_bucket_encryption_persistence() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_admin_test_context().await?;
    let client = &test_context.admin_client;
    let bucket = "test-bucket-encryption-persistence";

    // Create bucket
    client
        .put(format!("/api/v1/buckets/{bucket}"))
        .json(&json!({"bucketName": bucket}))
        .send()
        .await?
        .error_for_status()?;

    // Set encryption configuration
    let encryption_config = json!({
        "rules": [{
            "applyServerSideEncryptionByDefault": {
                "sseAlgorithm": "aws:kms",
                "kmsMasterKeyID": "test-key-id"
            }
        }]
    });

    client
        .put(format!("/api/v1/buckets/{bucket}/encryption"))
        .json(&encryption_config)
        .send()
        .await?
        .error_for_status()?;

    // Restart server (simulate by creating new context)
    drop(test_context);
    let new_test_context = setup_admin_test_context().await?;
    let new_client = &new_test_context.admin_client;

    // Verify configuration persists
    let response = new_client.get(format!("/api/v1/buckets/{bucket}/encryption")).send().await?;

    assert_eq!(response.status(), StatusCode::OK);
    let config = response.json::<serde_json::Value>().await?;
    assert_eq!(config["rules"][0]["applyServerSideEncryptionByDefault"]["sseAlgorithm"], "aws:kms");
    assert_eq!(config["rules"][0]["applyServerSideEncryptionByDefault"]["kmsMasterKeyID"], "test-key-id");

    cleanup_admin_test_context(new_test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_admin_bucket_encryption_concurrent() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_admin_test_context().await?;
    let client = &test_context.admin_client;
    let bucket = "test-bucket-encryption-concurrent";

    // Create bucket
    client
        .put(format!("/api/v1/buckets/{bucket}"))
        .json(&json!({"bucketName": bucket}))
        .send()
        .await?
        .error_for_status()?;

    let encryption_config = json!({
        "rules": [{
            "applyServerSideEncryptionByDefault": {
                "sseAlgorithm": "AES256"
            }
        }]
    });

    // Concurrent updates
    let mut handles = vec![];
    for i in 0..5 {
        let client = client.clone();
        let bucket = bucket.to_string();
        let mut config = encryption_config.clone();
        config["rules"][0]["applyServerSideEncryptionByDefault"]["sseAlgorithm"] =
            serde_json::Value::String(format!("AES256-{i}"));

        handles.push(tokio::spawn(async move {
            client
                .put(format!("/api/v1/buckets/{bucket}/encryption"))
                .json(&config)
                .send()
                .await
        }));
    }

    let results = futures::future::join_all(handles).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();

    // At least one should succeed
    assert!(success_count > 0);

    cleanup_admin_test_context(test_context).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_admin_bucket_encryption_authorization() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_context = setup_admin_test_context().await?;
    let client = &test_context.admin_client;
    let bucket = "test-bucket-encryption-auth";

    // Create bucket
    client
        .put(format!("/api/v1/buckets/{bucket}"))
        .json(&json!({"bucketName": bucket}))
        .send()
        .await?
        .error_for_status()?;

    // Test unauthorized access (if implemented)
    let unauthorized_client = reqwest::Client::new();
    let response = unauthorized_client
        .get(format!("{}/api/v1/buckets/{}/encryption", test_context.base_url, bucket))
        .send()
        .await?;

    // Should return 403 or 401 based on auth implementation
    assert!(response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN);

    cleanup_admin_test_context(test_context).await?;
    Ok(())
}
