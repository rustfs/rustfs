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

//! Bulk DEK rekey sweep over stored objects.
//!
//! The full loop — rotate the master key, sweep, prove convergence — runs
//! against Vault Transit, whose context-bound envelopes exercise the
//! decrypt + re-encrypt rewrap route end to end. The capability refusal runs
//! against the Local backend, which supports no rewrap at all.

use super::common::{
    LocalKMSTestEnvironment, VAULT_KEY_NAME, VaultTestEnvironment, kms_admin_request, start_kms, wait_for_kms_ready,
};
use crate::common::{TEST_BUCKET, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use std::time::Duration;
use tracing::info;

async fn rekey_status(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let body = kms_admin_request(
        base_url,
        http::Method::GET,
        "/rustfs/admin/v3/kms/keys/rekey/status",
        None,
        access_key,
        secret_key,
    )
    .await?;
    Ok(serde_json::from_str(&body)?)
}

/// Start a sweep and poll it to a terminal state.
async fn run_rekey_to_completion(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
    request_body: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    kms_admin_request(
        base_url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/rekey",
        Some(request_body),
        access_key,
        secret_key,
    )
    .await?;

    for _ in 0..120 {
        let status = rekey_status(base_url, access_key, secret_key).await?;
        if status["state"] != "running" {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("rekey sweep did not reach a terminal state in time".into())
}

#[tokio::test]
async fn kms_rekey_sweep_rewraps_rotated_envelopes_and_converges() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Testing the bulk rekey sweep against Vault Transit");

    let mut env = VaultTestEnvironment::new().await?;
    env.start_vault().await?;
    env.setup_vault_transit().await?;
    env.start_rustfs_for_vault().await?;
    env.configure_vault_transit_kms().await?;
    start_kms(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;
    wait_for_kms_ready(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;

    let base_url = env.base_env.url.clone();
    let access_key = env.base_env.access_key.clone();
    let secret_key = env.base_env.secret_key.clone();

    let s3_client = env.base_env.create_s3_client();
    env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Three encrypted objects the sweep must rewrap, one plaintext object it
    // must leave alone.
    let encrypted_keys = ["rekey/alpha", "rekey/beta", "rekey/gamma"];
    let mut bodies = Vec::new();
    for (index, key) in encrypted_keys.iter().enumerate() {
        let body: Vec<u8> = (0..2048).map(|i| ((i + index * 7) % 251) as u8).collect();
        s3_client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(*key)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(VAULT_KEY_NAME)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
        bodies.push(body);
    }
    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("rekey/plaintext")
        .body(ByteStream::from(b"unencrypted".to_vec()))
        .send()
        .await?;

    // Rotate the master key so the stored envelopes fall behind Vault's
    // latest version.
    kms_admin_request(
        &base_url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/rotate",
        Some(&format!(r#"{{"key_id":"{VAULT_KEY_NAME}"}}"#)),
        &access_key,
        &secret_key,
    )
    .await?;

    let status =
        run_rekey_to_completion(&base_url, &access_key, &secret_key, &format!(r#"{{"buckets":["{TEST_BUCKET}"]}}"#)).await?;
    assert_eq!(status["state"], "completed", "first sweep must complete: {status}");
    assert_eq!(status["failed"], 0, "no object may fail: {status}");
    assert_eq!(
        status["rewrapped"],
        encrypted_keys.len(),
        "every rotated envelope must be rewrapped: {status}"
    );
    assert!(
        status["not_applicable"].as_u64().unwrap_or(0) >= 1,
        "the plaintext object must be reported not applicable: {status}"
    );

    // The rewrapped objects still serve their exact bytes.
    for (key, expected) in encrypted_keys.iter().zip(&bodies) {
        let response = s3_client.get_object().bucket(TEST_BUCKET).key(*key).send().await?;
        let data = response.body.collect().await?.into_bytes();
        assert_eq!(data.as_ref(), expected.as_slice(), "object {key} must be byte-exact after the rewrap");
    }

    // Convergence: a second sweep finds everything current and writes nothing.
    let status =
        run_rekey_to_completion(&base_url, &access_key, &secret_key, &format!(r#"{{"buckets":["{TEST_BUCKET}"]}}"#)).await?;
    assert_eq!(status["state"], "completed", "second sweep must complete: {status}");
    assert_eq!(status["rewrapped"], 0, "a converged sweep must write nothing: {status}");
    assert_eq!(status["failed"], 0, "{status}");
    assert_eq!(
        status["already_current"],
        encrypted_keys.len(),
        "every envelope must now be current: {status}"
    );

    env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    Ok(())
}

#[tokio::test]
async fn kms_rekey_refuses_a_backend_without_rewrap_support() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Testing that the rekey sweep refuses the Local backend up front");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let error = kms_admin_request(
        &kms_env.base_env.url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/rekey",
        Some("{}"),
        &kms_env.base_env.access_key,
        &kms_env.base_env.secret_key,
    )
    .await
    .expect_err("a backend without rewrap support must be refused up front");
    assert!(error.to_string().contains("501"), "the refusal must be 501 Not Implemented, got: {error}");

    Ok(())
}
