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

//! Core WebDAV tests

use crate::common::local_http_client;
use crate::common::rustfs_binary_path_with_features;
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::Result;
use base64::Engine;
use http::header::{CONTENT_TYPE, HOST};
use reqwest::Client;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use tokio::process::Command;
use tracing::info;

// Fixed WebDAV port for testing
const WEBDAV_PORT: u16 = 9080;
const WEBDAV_ADDRESS: &str = "127.0.0.1:9080";
const S3_TEST_ADDRESS: &str = "127.0.0.1:9010";

/// Create HTTP client with basic auth
fn create_client() -> Client {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("Failed to create HTTP client")
}

/// Get basic auth header value
fn basic_auth_header() -> String {
    basic_auth_header_for(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)
}

fn basic_auth_header_for(access_key: &str, secret_key: &str) -> String {
    let credentials = format!("{}:{}", access_key, secret_key);
    let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
    format!("Basic {}", encoded)
}

async fn signed_admin_request(
    method: http::Method,
    url: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow::anyhow!("request URL missing authority"))?
        .to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(
        request.body(Body::empty())?,
        content_len,
        DEFAULT_ACCESS_KEY,
        DEFAULT_SECRET_KEY,
        "",
        "us-east-1",
    );

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let mut request_builder = local_http_client().request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

async fn admin_create_user(base_url: &str, username: &str, secret_key: &str) -> Result<()> {
    let url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", base_url, username);
    let body = serde_json::json!({
        "secretKey": secret_key,
        "status": "enabled"
    });
    let response =
        signed_admin_request(http::Method::PUT, &url, Some(body.to_string().into_bytes()), Some("application/json")).await?;

    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("create user failed: {status} {body}");
    }

    Ok(())
}

async fn admin_add_canned_policy(base_url: &str, policy_name: &str, policy: &serde_json::Value) -> Result<()> {
    let url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", base_url, policy_name);
    let response =
        signed_admin_request(http::Method::PUT, &url, Some(policy.to_string().into_bytes()), Some("application/json")).await?;

    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("add canned policy failed: {status} {body}");
    }

    Ok(())
}

async fn admin_attach_policy_to_user(base_url: &str, policy_name: &str, username: &str) -> Result<()> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        base_url, policy_name, username
    );
    let response = signed_admin_request(http::Method::PUT, &url, Some(Vec::new()), None).await?;

    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("attach policy failed: {status} {body}");
    }

    Ok(())
}

/// Test WebDAV: MKCOL (create bucket), PUT, GET, DELETE, PROPFIND operations
pub async fn test_webdav_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;
    let admin_base_url = format!("http://{}", S3_TEST_ADDRESS);

    // Start server manually
    info!("Starting WebDAV server on {}", WEBDAV_ADDRESS);
    let binary_path = rustfs_binary_path_with_features(Some("webdav"));
    let mut server_process = Command::new(&binary_path)
        .arg("--address")
        .arg(S3_TEST_ADDRESS)
        .env("RUSTFS_WEBDAV_ENABLE", "true")
        .env("RUSTFS_WEBDAV_ADDRESS", WEBDAV_ADDRESS)
        .env("RUSTFS_WEBDAV_TLS_ENABLED", "false") // No TLS for testing
        .arg(&env.temp_dir)
        .spawn()?;

    // Ensure server is cleaned up even on failure
    let result = async {
        // Wait for server to be ready
        ProtocolTestEnvironment::wait_for_port_ready(WEBDAV_PORT, 30)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let client = create_client();
        let auth_header = basic_auth_header();
        let base_url = format!("http://{}", WEBDAV_ADDRESS);

        // Test PROPFIND at root (list buckets)
        info!("Testing WebDAV: PROPFIND at root (list buckets)");
        let resp = client
            .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), &base_url)
            .header("Authorization", &auth_header)
            .header("Depth", "1")
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 207,
            "PROPFIND at root should succeed, got: {}",
            resp.status()
        );
        info!("PASS: PROPFIND at root successful");

        // Test MKCOL (create bucket)
        let bucket_name = "webdav-test-bucket";
        info!("Testing WebDAV: MKCOL (create bucket '{}')", bucket_name);
        let resp = client
            .request(reqwest::Method::from_bytes(b"MKCOL").unwrap(), format!("{}/{}", base_url, bucket_name))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "MKCOL should succeed, got: {}",
            resp.status()
        );
        info!("PASS: MKCOL bucket '{}' successful", bucket_name);

        // Test PUT (upload file)
        let filename = "test-file.txt";
        let file_content = "Hello, WebDAV!";
        info!("Testing WebDAV: PUT (upload file '{}')", filename);
        let resp = client
            .put(format!("{}/{}/{}", base_url, bucket_name, filename))
            .header("Authorization", &auth_header)
            .body(file_content)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "PUT should succeed, got: {}",
            resp.status()
        );
        info!("PASS: PUT file '{}' successful", filename);

        // Test GET (download file)
        info!("Testing WebDAV: GET (download file '{}')", filename);
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(resp.status().is_success(), "GET should succeed, got: {}", resp.status());
        let downloaded_content = resp.text().await?;
        assert_eq!(downloaded_content, file_content, "Downloaded content should match uploaded content");
        info!("PASS: GET file '{}' successful, content matches", filename);

        // Test PROPFIND on bucket (list objects)
        info!("Testing WebDAV: PROPFIND on bucket (list objects)");
        let resp = client
            .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), format!("{}/{}", base_url, bucket_name))
            .header("Authorization", &auth_header)
            .header("Depth", "1")
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 207,
            "PROPFIND on bucket should succeed, got: {}",
            resp.status()
        );
        let body = resp.text().await?;
        assert!(body.contains(filename), "File should appear in PROPFIND response");
        info!("PASS: PROPFIND on bucket successful, file '{}' found", filename);

        // Test DELETE file
        info!("Testing WebDAV: DELETE file '{}'", filename);
        let resp = client
            .delete(format!("{}/{}/{}", base_url, bucket_name, filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 204,
            "DELETE file should succeed, got: {}",
            resp.status()
        );
        info!("PASS: DELETE file '{}' successful", filename);

        // Verify file is deleted
        info!("Testing WebDAV: Verify file is deleted");
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().as_u16() == 404,
            "GET deleted file should return 404, got: {}",
            resp.status()
        );
        info!("PASS: Verified file '{}' is deleted", filename);

        // Test MOVE (rename) file
        info!("Testing WebDAV: PUT file for MOVE test");
        let move_filename = "move-source.txt";
        let move_dest_filename = "move-dest.txt";
        let move_content = "File to be moved!";
        let resp = client
            .put(format!("{}/{}/{}", base_url, bucket_name, move_filename))
            .header("Authorization", &auth_header)
            .body(move_content)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "PUT for MOVE test should succeed, got: {}",
            resp.status()
        );
        info!("PASS: PUT file '{}' for MOVE test successful", move_filename);

        // Execute MOVE request
        info!("Testing WebDAV: MOVE file '{}' to '{}'", move_filename, move_dest_filename);
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MOVE").unwrap(),
                format!("{}/{}/{}", base_url, bucket_name, move_filename),
            )
            .header("Authorization", &auth_header)
            .header("Destination", format!("/{}/{}", bucket_name, move_dest_filename))
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 204 || resp.status().as_u16() == 201,
            "MOVE should succeed, got: {}",
            resp.status()
        );
        info!(
            "PASS: MOVE file '{}' to '{}' successful (HTTP {})",
            move_filename,
            move_dest_filename,
            resp.status()
        );

        // Verify source file is gone
        info!("Testing WebDAV: Verify source '{}' is deleted after MOVE", move_filename);
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, move_filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().as_u16() == 404,
            "GET moved source should return 404, got: {}",
            resp.status()
        );
        info!("PASS: Source '{}' is deleted after MOVE", move_filename);

        // Verify destination file exists and content matches
        info!("Testing WebDAV: Verify destination '{}' has correct content", move_dest_filename);
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, move_dest_filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success(),
            "GET destination after MOVE should succeed, got: {}",
            resp.status()
        );
        let moved_content = resp.text().await?;
        assert_eq!(moved_content, move_content, "Moved file content should match original");
        info!("PASS: Destination '{}' has correct content after MOVE", move_dest_filename);

        // Test directory creation and rename
        info!("Testing WebDAV: MKCOL directory");
        let dir_name = "test-directory";
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MKCOL").unwrap(),
                format!("{}/{}/{}", base_url, bucket_name, dir_name),
            )
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "MKCOL directory should succeed, got: {}",
            resp.status()
        );
        info!("PASS: MKCOL directory successful");

        // Upload file into directory
        let dir_filename = "dir-file.txt";
        let dir_file_content = "File inside test directory!";
        let resp = client
            .put(format!("{}/{}/{}/{}", base_url, bucket_name, dir_name, dir_filename))
            .header("Authorization", &auth_header)
            .body(dir_file_content)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "PUT file into directory should succeed, got: {}",
            resp.status()
        );
        info!("PASS: PUT file into directory successful");

        // Test PROPFIND on directory
        info!("Testing WebDAV: PROPFIND directory");
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"PROPFIND").unwrap(),
                format!("{}/{}/{}", base_url, bucket_name, dir_name),
            )
            .header("Authorization", &auth_header)
            .header("Depth", "1")
            .send()
            .await?;
        assert!(resp.status().is_success(), "PROPFIND directory should succeed, got: {}", resp.status());
        let propfind_body = resp.text().await?;
        assert!(propfind_body.contains(dir_filename), "PROPFIND should list file in directory");
        info!("PASS: PROPFIND directory successful, file listed correctly");

        // Current WebDAV support exposes collection listing via PROPFIND; GET on a collection is not implemented.
        info!("Testing WebDAV: GET directory listing fallback behavior");
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, dir_name))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert_eq!(
            resp.status().as_u16(),
            405,
            "GET on a WebDAV collection should currently return 405, got: {}",
            resp.status()
        );
        info!("PASS: GET collection correctly returned 405; PROPFIND remains the listing path");

        // Rename directory
        info!("Testing WebDAV: MOVE directory");
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MOVE").unwrap(),
                format!("{}/{}/{}", base_url, bucket_name, dir_name),
            )
            .header("Authorization", &auth_header)
            .header("Destination", format!("/{}/renamed-dir", bucket_name))
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 204 || resp.status().as_u16() == 201,
            "MOVE directory should succeed, got: {}",
            resp.status()
        );
        info!("PASS: MOVE directory successful");

        // Verify source directory is gone
        info!("Testing WebDAV: Verify source directory is deleted after MOVE");
        let resp = client
            .get(format!("{}/{}/{}", base_url, bucket_name, dir_name))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().as_u16() == 404,
            "GET moved directory should return 404, got: {}",
            resp.status()
        );
        info!("PASS: Source directory is deleted after MOVE");

        // Verify renamed directory exists and file content matches
        info!("Testing WebDAV: Verify file in renamed directory");
        let resp = client
            .get(format!("{}/{}/renamed-dir/{}", base_url, bucket_name, dir_filename))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success(),
            "GET file in renamed directory should succeed, got: {}",
            resp.status()
        );
        let renamed_dir_content = resp.text().await?;
        assert_eq!(renamed_dir_content, dir_file_content, "File content in renamed directory should match");
        info!("PASS: File in renamed directory has correct content");

        // Test nested directory creation and rename
        info!("Testing WebDAV: MKCOL nested directory");
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MKCOL").unwrap(),
                format!("{}/{}/renamed-dir/nested-dir", base_url, bucket_name),
            )
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "MKCOL nested directory should succeed, got: {}",
            resp.status()
        );
        info!("PASS: MKCOL nested directory successful");

        // Upload file into nested directory
        let nested_file_content = "File in nested directory!";
        let resp = client
            .put(format!("{}/{}/renamed-dir/nested-dir/nested-file.txt", base_url, bucket_name))
            .header("Authorization", &auth_header)
            .body(nested_file_content)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "PUT file into nested directory should succeed, got: {}",
            resp.status()
        );

        // Rename nested directory
        info!("Testing WebDAV: MOVE nested directory");
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MOVE").unwrap(),
                format!("{}/{}/renamed-dir/nested-dir", base_url, bucket_name),
            )
            .header("Authorization", &auth_header)
            .header("Destination", format!("/{}/renamed-dir/new-nested-dir", bucket_name))
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 204 || resp.status().as_u16() == 201,
            "MOVE nested directory should succeed, got: {}",
            resp.status()
        );
        info!("PASS: MOVE nested directory successful");

        // Verify nested file after rename
        let resp = client
            .get(format!("{}/{}/renamed-dir/new-nested-dir/nested-file.txt", base_url, bucket_name))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success(),
            "GET file in renamed nested directory should succeed, got: {}",
            resp.status()
        );
        let nested_content = resp.text().await?;
        assert_eq!(
            nested_content, nested_file_content,
            "File content in renamed nested directory should match"
        );
        info!("PASS: File in renamed nested directory has correct content");

        // Test directory MOVE authz failure does not create partial destination writes
        info!("Testing WebDAV: directory MOVE denied by missing DeleteObject must not create partial writes");
        let restricted_bucket = "webdav-authz-bucket";
        let restricted_dir = "restricted-src";
        let restricted_dst = "restricted-dst";
        let restricted_file = "locked.txt";
        let restricted_content = "must remain only at source";
        let restricted_user = "webdav-limited-user";
        let restricted_secret = "webdav-limited-secret";
        let restricted_policy_name = "webdav-move-no-delete";

        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MKCOL").unwrap(),
                format!("{}/{}", base_url, restricted_bucket),
            )
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "MKCOL restricted bucket should succeed, got: {}",
            resp.status()
        );

        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MKCOL").unwrap(),
                format!("{}/{}/{}", base_url, restricted_bucket, restricted_dir),
            )
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "MKCOL restricted directory should succeed, got: {}",
            resp.status()
        );

        let resp = client
            .put(format!("{}/{}/{}/{}", base_url, restricted_bucket, restricted_dir, restricted_file))
            .header("Authorization", &auth_header)
            .body(restricted_content)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 201,
            "PUT restricted file should succeed, got: {}",
            resp.status()
        );

        admin_create_user(&admin_base_url, restricted_user, restricted_secret).await?;
        admin_add_canned_policy(
            &admin_base_url,
            restricted_policy_name,
            &serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": [format!("arn:aws:s3:::{}", restricted_bucket)]
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": [format!("arn:aws:s3:::{}/*", restricted_bucket)]
                    }
                ]
            }),
        )
        .await?;
        admin_attach_policy_to_user(&admin_base_url, restricted_policy_name, restricted_user).await?;

        let restricted_auth = basic_auth_header_for(restricted_user, restricted_secret);
        let resp = client
            .request(
                reqwest::Method::from_bytes(b"MOVE").unwrap(),
                format!("{}/{}/{}", base_url, restricted_bucket, restricted_dir),
            )
            .header("Authorization", &restricted_auth)
            .header("Destination", format!("/{}/{}", restricted_bucket, restricted_dst))
            .send()
            .await?;
        assert!(
            !resp.status().is_success(),
            "MOVE without DeleteObject permission should be rejected, got: {}",
            resp.status()
        );

        let resp = client
            .get(format!("{}/{}/{}/{}", base_url, restricted_bucket, restricted_dst, restricted_file))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert_eq!(
            resp.status().as_u16(),
            404,
            "Denied MOVE must not create destination object, got: {}",
            resp.status()
        );

        let resp = client
            .get(format!("{}/{}/{}/{}", base_url, restricted_bucket, restricted_dir, restricted_file))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success(),
            "Source object should remain after denied MOVE, got: {}",
            resp.status()
        );
        assert_eq!(resp.text().await?, restricted_content, "Denied MOVE must leave source content untouched");
        info!("PASS: denied directory MOVE left source intact and created no destination objects");

        // Test DELETE bucket
        info!("Testing WebDAV: DELETE bucket '{}'", bucket_name);
        let resp = client
            .delete(format!("{}/{}", base_url, bucket_name))
            .header("Authorization", &auth_header)
            .send()
            .await?;
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 204,
            "DELETE bucket should succeed, got: {}",
            resp.status()
        );
        info!("PASS: DELETE bucket '{}' successful", bucket_name);

        // Test authentication failure
        info!("Testing WebDAV: Authentication failure");
        let resp = client
            .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), &base_url)
            .header("Authorization", "Basic aW52YWxpZDppbnZhbGlk") // invalid:invalid
            .send()
            .await?;
        assert_eq!(resp.status().as_u16(), 401, "Invalid auth should return 401, got: {}", resp.status());
        info!("PASS: Authentication failure test successful");

        info!("WebDAV core tests passed");
        Ok(())
    }
    .await;

    // Always cleanup server process
    let _ = server_process.kill().await;
    let _ = server_process.wait().await;

    result
}

#[tokio::test]
#[serial]
async fn test_webdav_core_operations_direct() -> Result<()> {
    test_webdav_core_operations().await
}
