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

use crate::common::rustfs_binary_path_with_features;
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::Result;
use base64::Engine;
use reqwest::Client;
use tokio::process::Command;
use tracing::info;

// Fixed WebDAV port for testing
const WEBDAV_PORT: u16 = 9080;
const WEBDAV_ADDRESS: &str = "127.0.0.1:9080";

/// Create HTTP client with basic auth
fn create_client() -> Client {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("Failed to create HTTP client")
}

/// Get basic auth header value
fn basic_auth_header() -> String {
    let credentials = format!("{}:{}", DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY);
    let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
    format!("Basic {}", encoded)
}

/// Test WebDAV: MKCOL (create bucket), PUT, GET, DELETE, PROPFIND operations
pub async fn test_webdav_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Start server manually
    info!("Starting WebDAV server on {}", WEBDAV_ADDRESS);
    let binary_path = rustfs_binary_path_with_features(Some("ftps,webdav"));
    let mut server_process = Command::new(&binary_path)
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
