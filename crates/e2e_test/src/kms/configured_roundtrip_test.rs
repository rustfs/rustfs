// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configured-backend validation for the KMS admin and SSE-KMS round-trip
//! contract tracked by rustfs/backlog#1378.

use super::common::{
    LocalKMSTestEnvironment, VAULT_KEY_NAME, VaultTestEnvironment, configure_kms, get_kms_status, kms_admin_request, start_kms,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, ServerSideEncryption, VersioningConfiguration};
use serial_test::serial;
use std::error::Error;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

async fn assert_configured_status(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
    expected_backend: &str,
    expected_default_key: &str,
) -> TestResult {
    let body = get_kms_status(base_url, access_key, secret_key).await?;
    let status: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(status["backend_type"], expected_backend);
    assert_eq!(status["backend_status"], "healthy");
    assert_eq!(status["default_key_id"], expected_default_key);
    Ok(())
}

async fn create_and_verify_key(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let create_body = serde_json::json!({
        "key_usage": "EncryptDecrypt",
        "description": "configured KMS round-trip e2e key",
        "tags": {
            "test": "backlog-1378"
        }
    })
    .to_string();
    let created = kms_admin_request(
        base_url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys",
        Some(&create_body),
        access_key,
        secret_key,
    )
    .await?;
    let created: serde_json::Value = serde_json::from_str(&created)?;
    assert_eq!(created["success"], true);
    let key_id = created["key_id"]
        .as_str()
        .ok_or("create KMS key response omitted key_id")?
        .to_string();

    let described = kms_admin_request(
        base_url,
        http::Method::GET,
        &format!("/rustfs/admin/v3/kms/keys/{key_id}"),
        None,
        access_key,
        secret_key,
    )
    .await?;
    let described: serde_json::Value = serde_json::from_str(&described)?;
    assert_eq!(described["success"], true);
    assert_eq!(described["key_metadata"]["key_id"], key_id);
    assert_eq!(described["key_metadata"]["key_state"], "Enabled");

    let listed =
        kms_admin_request(base_url, http::Method::GET, "/rustfs/admin/v3/kms/keys", None, access_key, secret_key).await?;
    let listed: serde_json::Value = serde_json::from_str(&listed)?;
    let keys = listed["keys"].as_array().ok_or("list KMS keys response omitted keys")?;
    assert!(keys.iter().any(|key| key["key_id"] == key_id), "created KMS key must appear in list");
    Ok(key_id)
}

async fn assert_key_deletion_lifecycle(base_url: &str, access_key: &str, secret_key: &str, key_id: &str) -> TestResult {
    let scheduled = kms_admin_request(
        base_url,
        http::Method::DELETE,
        "/rustfs/admin/v3/kms/keys/delete",
        Some(
            &serde_json::json!({
                "key_id": key_id,
                "pending_window_in_days": 7,
                "force_immediate": false
            })
            .to_string(),
        ),
        access_key,
        secret_key,
    )
    .await?;
    let scheduled: serde_json::Value = serde_json::from_str(&scheduled)?;
    assert_eq!(scheduled["success"], true);
    assert!(scheduled["deletion_date"].is_string());

    let cancelled = kms_admin_request(
        base_url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/cancel-deletion",
        Some(&serde_json::json!({ "key_id": key_id }).to_string()),
        access_key,
        secret_key,
    )
    .await?;
    let cancelled: serde_json::Value = serde_json::from_str(&cancelled)?;
    assert_eq!(cancelled["success"], true);
    assert_eq!(cancelled["key_metadata"]["key_state"], "Enabled");

    let removed = kms_admin_request(
        base_url,
        http::Method::DELETE,
        "/rustfs/admin/v3/kms/keys/delete",
        Some(
            &serde_json::json!({
                "key_id": key_id,
                "force_immediate": true
            })
            .to_string(),
        ),
        access_key,
        secret_key,
    )
    .await?;
    let removed: serde_json::Value = serde_json::from_str(&removed)?;
    assert_eq!(removed["success"], true);

    let listed =
        kms_admin_request(base_url, http::Method::GET, "/rustfs/admin/v3/kms/keys", None, access_key, secret_key).await?;
    let listed: serde_json::Value = serde_json::from_str(&listed)?;
    assert_eq!(listed["success"], true);
    let keys = listed["keys"]
        .as_array()
        .ok_or("list KMS keys response omitted keys after deletion")?;
    if let Some(key) = keys.iter().find(|key| key["key_id"] == key_id) {
        assert_eq!(key["status"], "PendingDeletion", "a retained force-deleted key must be pending deletion");
        let removed = kms_admin_request(
            base_url,
            http::Method::DELETE,
            "/rustfs/admin/v3/kms/keys/delete",
            Some(
                &serde_json::json!({
                    "key_id": key_id,
                    "force_immediate": true
                })
                .to_string(),
            ),
            access_key,
            secret_key,
        )
        .await?;
        let removed: serde_json::Value = serde_json::from_str(&removed)?;
        assert_eq!(removed["success"], true);
    }

    let listed =
        kms_admin_request(base_url, http::Method::GET, "/rustfs/admin/v3/kms/keys", None, access_key, secret_key).await?;
    let listed: serde_json::Value = serde_json::from_str(&listed)?;
    assert_eq!(listed["success"], true);
    let keys = listed["keys"]
        .as_array()
        .ok_or("final list KMS keys response omitted keys after deletion")?;
    assert!(
        keys.iter().all(|key| key["key_id"] != key_id),
        "force-deleted KMS key must no longer appear in list"
    );
    Ok(())
}

async fn assert_versioned_sse_kms_roundtrip_and_cleanup(
    env: &crate::common::RustFSTestEnvironment,
    key_id: &str,
    bucket_prefix: &str,
) -> TestResult {
    let client = env.create_s3_client();
    let bucket = format!("{bucket_prefix}-{}", Uuid::new_v4().simple());
    let object = format!("configured-kms-probe/{}/object", Uuid::new_v4().simple());
    let first_body = b"configured KMS version one";
    let second_body = b"configured KMS version two";

    client.create_bucket().bucket(&bucket).send().await?;
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    let first = client
        .put_object()
        .bucket(&bucket)
        .key(&object)
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(key_id)
        .body(ByteStream::from_static(first_body))
        .send()
        .await?;
    assert_eq!(first.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
    assert_eq!(first.ssekms_key_id(), Some(key_id));
    let first_version = first.version_id().ok_or("first SSE-KMS PUT omitted version_id")?.to_string();

    let second = client
        .put_object()
        .bucket(&bucket)
        .key(&object)
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(key_id)
        .body(ByteStream::from_static(second_body))
        .send()
        .await?;
    assert_eq!(second.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
    assert_eq!(second.ssekms_key_id(), Some(key_id));
    let second_version = second
        .version_id()
        .ok_or("second SSE-KMS PUT omitted version_id")?
        .to_string();
    assert_ne!(first_version, second_version);
    let storage_root = std::path::Path::new(&env.temp_dir);
    super::encryption_metadata_test::assert_storage_encrypted(storage_root, &bucket, &object, first_body);
    super::encryption_metadata_test::assert_storage_encrypted(storage_root, &bucket, &object, second_body);

    for (version_id, expected) in [
        (&first_version, first_body.as_slice()),
        (&second_version, second_body.as_slice()),
    ] {
        let response = client
            .get_object()
            .bucket(&bucket)
            .key(&object)
            .version_id(version_id)
            .send()
            .await?;
        assert_eq!(response.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
        assert_eq!(response.ssekms_key_id(), Some(key_id));
        let actual = response.body.collect().await?.into_bytes();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.as_ref(), expected);
    }

    let marker = client.delete_object().bucket(&bucket).key(&object).send().await?;
    assert_eq!(marker.delete_marker(), Some(true));
    assert!(marker.version_id().is_some(), "versioned delete must create a delete marker");

    let before_cleanup = client.list_object_versions().bucket(&bucket).prefix(&object).send().await?;
    assert_eq!(
        before_cleanup
            .versions()
            .iter()
            .filter(|version| version.key() == Some(object.as_str()))
            .count(),
        2
    );
    assert_eq!(
        before_cleanup
            .delete_markers()
            .iter()
            .filter(|delete_marker| delete_marker.key() == Some(object.as_str()))
            .count(),
        1
    );

    client
        .delete_object()
        .bucket(&bucket)
        .key(&object)
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "true");
        })
        .send()
        .await?;

    let after_cleanup = client.list_object_versions().bucket(&bucket).prefix(&object).send().await?;
    assert!(
        after_cleanup
            .versions()
            .iter()
            .all(|version| version.key() != Some(object.as_str())),
        "force cleanup must remove every encrypted object version"
    );
    assert!(
        after_cleanup
            .delete_markers()
            .iter()
            .all(|delete_marker| delete_marker.key() != Some(object.as_str())),
        "force cleanup must remove the delete marker"
    );
    let head_error = match client.head_object().bucket(&bucket).key(&object).send().await {
        Ok(_) => return Err("force-cleaned probe object remained readable".into()),
        Err(error) => error,
    };
    let service_error = head_error
        .as_service_error()
        .ok_or_else(|| format!("force-cleaned HEAD failed with a non-service error: {head_error}"))?;
    assert!(
        service_error.is_not_found(),
        "force-cleaned HEAD returned the wrong service error: {service_error:?}"
    );

    client.delete_bucket().bucket(&bucket).send().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_configured_local_kms_admin_and_versioned_cleanup() -> TestResult {
    let mut env = LocalKMSTestEnvironment::new().await?;
    env.base_env.start_rustfs_server(Vec::new()).await?;

    let start_error = match start_kms(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await {
        Ok(()) => return Err("unconfigured KMS start unexpectedly succeeded".into()),
        Err(error) => error,
    };
    assert!(
        start_error.to_string().contains("no configuration provided"),
        "unconfigured KMS start returned the wrong business error: {start_error}"
    );

    let insecure_config = serde_json::json!({
        "backend_type": "Local",
        "key_dir": env.kms_keys_dir,
        "file_permissions": 0o600,
        "default_key_id": "rustfs-e2e-test-default-key"
    })
    .to_string();
    let configure_error =
        match configure_kms(&env.base_env.url, &insecure_config, &env.base_env.access_key, &env.base_env.secret_key).await {
            Ok(()) => return Err("insecure Local KMS configuration unexpectedly succeeded".into()),
            Err(error) => error,
        };
    assert!(
        configure_error.to_string().contains("requires a master key"),
        "invalid Local KMS configuration returned the wrong business error: {configure_error}"
    );

    let default_key_id = env.configure_local_kms().await?;
    start_kms(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;
    assert_configured_status(
        &env.base_env.url,
        &env.base_env.access_key,
        &env.base_env.secret_key,
        "local",
        &default_key_id,
    )
    .await?;
    let key_id = create_and_verify_key(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;
    assert_versioned_sse_kms_roundtrip_and_cleanup(&env.base_env, &key_id, "kms-local-configured").await?;
    assert_key_deletion_lifecycle(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key, &key_id).await?;
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires a Vault binary"]
async fn test_configured_vault_kms_admin_and_versioned_cleanup() -> TestResult {
    let mut env = VaultTestEnvironment::new().await?;
    env.start_vault().await?;
    env.setup_vault_transit().await?;
    env.start_rustfs_for_vault().await?;
    env.configure_vault_transit_kms().await?;
    start_kms(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;
    assert_configured_status(
        &env.base_env.url,
        &env.base_env.access_key,
        &env.base_env.secret_key,
        "vault-transit",
        VAULT_KEY_NAME,
    )
    .await?;

    let key_id = create_and_verify_key(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;
    assert_ne!(key_id, VAULT_KEY_NAME, "key lifecycle test must create a distinct Vault key");
    assert_versioned_sse_kms_roundtrip_and_cleanup(&env.base_env, &key_id, "kms-vault-configured").await?;
    assert_key_deletion_lifecycle(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key, &key_id).await?;
    Ok(())
}
