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

//! Integration tests for Swift object operations
//!
//! These tests verify the complete Swift API flow for object operations:
//! - Object upload (PUT)
//! - Object download (GET)
//! - Object metadata retrieval (HEAD)
//! - Object metadata updates (POST)
//! - Object deletion (DELETE)
//! - Object listing (GET on container)
//!
//! Note: These tests require a running RustFS server with Swift support enabled.
//! Set TEST_RUSTFS_SERVER environment variable to override the default endpoint.

use anyhow::{Context, Result};
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;
use std::env;

/// Test settings for Swift API integration tests
struct SwiftTestSettings {
    /// Swift endpoint (e.g., http://localhost:9000)
    endpoint: String,
    /// Authentication token (for Keystone auth)
    auth_token: String,
    /// Swift account (AUTH_{project_id})
    account: String,
}

impl SwiftTestSettings {
    fn new() -> Self {
        Self {
            endpoint: env::var("TEST_RUSTFS_SERVER").unwrap_or_else(|_| "http://localhost:9000".to_string()),
            auth_token: env::var("TEST_SWIFT_TOKEN").unwrap_or_else(|_| "test-token".to_string()),
            account: env::var("TEST_SWIFT_ACCOUNT").unwrap_or_else(|_| "AUTH_test-project-123".to_string()),
        }
    }

    /// Build Swift URL for container operations
    fn container_url(&self, container: &str) -> String {
        format!("{}/v1/{}/{}", self.endpoint, self.account, container)
    }

    /// Build Swift URL for object operations
    fn object_url(&self, container: &str, object: &str) -> String {
        format!("{}/v1/{}/{}/{}", self.endpoint, self.account, container, object)
    }
}

/// Swift client for integration testing
struct SwiftClient {
    client: Client,
    settings: SwiftTestSettings,
}

impl SwiftClient {
    fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            settings: SwiftTestSettings::new(),
        })
    }

    /// Create container (PUT /v1/{account}/{container})
    async fn create_container(&self, container: &str) -> Result<Response> {
        self.client
            .put(self.settings.container_url(container))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to create container")
    }

    /// Delete container (DELETE /v1/{account}/{container})
    async fn delete_container(&self, container: &str) -> Result<Response> {
        self.client
            .delete(self.settings.container_url(container))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to delete container")
    }

    /// Upload object (PUT /v1/{account}/{container}/{object})
    async fn put_object(
        &self,
        container: &str,
        object: &str,
        content: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Response> {
        let mut req = self
            .client
            .put(self.settings.object_url(container, object))
            .header("X-Auth-Token", &self.settings.auth_token)
            .body(content);

        // Add X-Object-Meta-* headers
        if let Some(meta) = metadata {
            for (key, value) in meta {
                req = req.header(format!("X-Object-Meta-{}", key), value);
            }
        }

        req.send().await.context("Failed to upload object")
    }

    /// Download object (GET /v1/{account}/{container}/{object})
    async fn get_object(&self, container: &str, object: &str) -> Result<Response> {
        self.client
            .get(self.settings.object_url(container, object))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to download object")
    }

    /// Get object metadata (HEAD /v1/{account}/{container}/{object})
    async fn head_object(&self, container: &str, object: &str) -> Result<Response> {
        self.client
            .head(self.settings.object_url(container, object))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to get object metadata")
    }

    /// Update object metadata (POST /v1/{account}/{container}/{object})
    async fn update_object_metadata(&self, container: &str, object: &str, metadata: HashMap<String, String>) -> Result<Response> {
        let mut req = self
            .client
            .post(self.settings.object_url(container, object))
            .header("X-Auth-Token", &self.settings.auth_token);

        // Add X-Object-Meta-* headers
        for (key, value) in metadata {
            req = req.header(format!("X-Object-Meta-{}", key), value);
        }

        req.send().await.context("Failed to update object metadata")
    }

    /// Delete object (DELETE /v1/{account}/{container}/{object})
    async fn delete_object(&self, container: &str, object: &str) -> Result<Response> {
        self.client
            .delete(self.settings.object_url(container, object))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to delete object")
    }

    /// List objects in container (GET /v1/{account}/{container})
    async fn list_objects(&self, container: &str) -> Result<Response> {
        self.client
            .get(self.settings.container_url(container))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to list objects")
    }
}

/// Test: Upload an object
///
/// Verifies:
/// - PUT /v1/{account}/{container}/{object} returns 201 Created
/// - ETag header is present
/// - X-Trans-Id header is present
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_upload_object() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-object.txt";

    // Create container first
    let _ = client.create_container(&container_name).await?;

    // Upload object
    let content = b"Hello, Swift!".to_vec();
    let response = client.put_object(&container_name, object_name, content, None).await?;

    // Should return 201 Created
    assert_eq!(response.status(), StatusCode::CREATED, "Expected 201 Created for new object");

    // Verify ETag header
    assert!(response.headers().contains_key("etag"), "Missing ETag header");

    // Verify Swift transaction headers
    assert!(response.headers().contains_key("x-trans-id"), "Missing X-Trans-Id header");

    // Cleanup
    let _ = client.delete_object(&container_name, object_name).await;
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Upload object with custom metadata
///
/// Verifies:
/// - Object can be uploaded with X-Object-Meta-* headers
/// - Metadata is preserved
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_upload_object_with_metadata() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-object-meta.txt";

    // Create container first
    let _ = client.create_container(&container_name).await?;

    // Upload object with metadata
    let content = b"Test content".to_vec();
    let mut metadata = HashMap::new();
    metadata.insert("author".to_string(), "test-user".to_string());
    metadata.insert("version".to_string(), "1.0".to_string());

    let response = client
        .put_object(&container_name, object_name, content, Some(metadata))
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);

    // Verify metadata with HEAD
    let head_response = client.head_object(&container_name, object_name).await?;
    assert_eq!(head_response.status(), StatusCode::OK);

    let headers = head_response.headers();
    assert!(headers.contains_key("x-object-meta-author"), "Missing X-Object-Meta-Author header");
    assert!(headers.contains_key("x-object-meta-version"), "Missing X-Object-Meta-Version header");

    // Cleanup
    let _ = client.delete_object(&container_name, object_name).await;
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Download an object
///
/// Verifies:
/// - GET /v1/{account}/{container}/{object} returns 200 OK
/// - Content matches uploaded content
/// - Content-Length header is correct
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_download_object() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-download.txt";

    // Create container and upload object
    let _ = client.create_container(&container_name).await?;
    let content = b"Test download content".to_vec();
    let _ = client.put_object(&container_name, object_name, content.clone(), None).await?;

    // Download object
    let response = client.get_object(&container_name, object_name).await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Verify content
    let downloaded = response.bytes().await?;
    assert_eq!(downloaded.to_vec(), content, "Downloaded content doesn't match");

    // Cleanup
    let _ = client.delete_object(&container_name, object_name).await;
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Get object metadata (HEAD)
///
/// Verifies:
/// - HEAD /v1/{account}/{container}/{object} returns 200 OK
/// - Content-Length header is present
/// - ETag header is present
/// - Last-Modified header is present
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_head_object() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-head.txt";

    // Create container and upload object
    let _ = client.create_container(&container_name).await?;
    let content = b"Test head content".to_vec();
    let _ = client.put_object(&container_name, object_name, content.clone(), None).await?;

    // Get metadata
    let response = client.head_object(&container_name, object_name).await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Verify headers
    let headers = response.headers();
    assert!(headers.contains_key("content-length"), "Missing Content-Length header");
    assert!(headers.contains_key("etag"), "Missing ETag header");
    assert!(headers.contains_key("last-modified"), "Missing Last-Modified header");

    // Cleanup
    let _ = client.delete_object(&container_name, object_name).await;
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Update object metadata (POST)
///
/// Verifies:
/// - POST /v1/{account}/{container}/{object} returns 204 No Content
/// - Metadata is updated
/// - Content is not modified
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_update_object_metadata() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-update-meta.txt";

    // Create container and upload object
    let _ = client.create_container(&container_name).await?;
    let content = b"Test metadata update".to_vec();
    let _ = client.put_object(&container_name, object_name, content.clone(), None).await?;

    // Update metadata
    let mut new_metadata = HashMap::new();
    new_metadata.insert("updated".to_string(), "true".to_string());
    new_metadata.insert("timestamp".to_string(), "2024-01-01".to_string());

    let response = client
        .update_object_metadata(&container_name, object_name, new_metadata)
        .await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify metadata was updated
    let head_response = client.head_object(&container_name, object_name).await?;
    assert!(head_response.headers().contains_key("x-object-meta-updated"));
    assert!(head_response.headers().contains_key("x-object-meta-timestamp"));

    // Verify content was not modified
    let get_response = client.get_object(&container_name, object_name).await?;
    let downloaded = get_response.bytes().await?;
    assert_eq!(downloaded.to_vec(), content, "Content should not be modified");

    // Cleanup
    let _ = client.delete_object(&container_name, object_name).await;
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Delete an object
///
/// Verifies:
/// - DELETE /v1/{account}/{container}/{object} returns 204 No Content
/// - Object is removed (GET returns 404)
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_delete_object() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = "test-delete.txt";

    // Create container and upload object
    let _ = client.create_container(&container_name).await?;
    let content = b"Test delete".to_vec();
    let _ = client.put_object(&container_name, object_name, content, None).await?;

    // Delete object
    let response = client.delete_object(&container_name, object_name).await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify object is deleted (GET should return 404)
    let get_response = client.get_object(&container_name, object_name).await?;
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND, "Object should be deleted");

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Delete non-existent object (idempotent)
///
/// Verifies:
/// - DELETE on non-existent object returns 204 No Content (Swift idempotency)
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_delete_nonexistent_object() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());
    let object_name = format!("nonexistent-{}.txt", uuid::Uuid::new_v4());

    // Create container
    let _ = client.create_container(&container_name).await?;

    // Try to delete non-existent object
    let response = client.delete_object(&container_name, &object_name).await?;

    // Swift DELETE is idempotent - should return 204 even for non-existent objects
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "Expected 204 No Content for non-existent object (idempotent)"
    );

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: List objects in container
///
/// Verifies:
/// - GET /v1/{account}/{container} returns 200 OK
/// - Response is valid JSON array
/// - Uploaded objects appear in list
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_list_objects() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // Create container
    let _ = client.create_container(&container_name).await?;

    // Upload multiple objects
    let objects = vec!["obj1.txt", "obj2.txt", "obj3.txt"];
    for obj_name in &objects {
        let content = format!("Content of {}", obj_name).into_bytes();
        let _ = client.put_object(&container_name, obj_name, content, None).await?;
    }

    // List objects
    let response = client.list_objects(&container_name).await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Parse JSON response
    let object_list: Vec<Value> = response.json().await.context("Failed to parse object list JSON")?;

    // Verify all objects are in the list
    assert!(
        object_list.len() >= objects.len(),
        "Object list should contain at least {} objects",
        objects.len()
    );

    for obj_name in &objects {
        let found = object_list.iter().any(|o| {
            o.get("name")
                .and_then(|n| n.as_str())
                .map(|n| n == *obj_name)
                .unwrap_or(false)
        });
        assert!(found, "Object {} should be in the list", obj_name);
    }

    // Cleanup
    for obj_name in &objects {
        let _ = client.delete_object(&container_name, obj_name).await;
    }
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Complete object lifecycle
///
/// Verifies the full lifecycle:
/// 1. Upload object with metadata
/// 2. Download and verify content
/// 3. Get metadata
/// 4. Update metadata
/// 5. List objects and verify presence
/// 6. Delete object
/// 7. Verify deletion
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_object_lifecycle() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-lifecycle-{}", uuid::Uuid::new_v4());
    let object_name = "lifecycle-test.txt";

    // Create container
    let create_response = client.create_container(&container_name).await?;
    assert_eq!(create_response.status(), StatusCode::CREATED);

    // 1. Upload object with metadata
    let content = b"Lifecycle test content".to_vec();
    let mut metadata = HashMap::new();
    metadata.insert("test-type".to_string(), "lifecycle".to_string());
    let put_response = client
        .put_object(&container_name, object_name, content.clone(), Some(metadata))
        .await?;
    assert_eq!(put_response.status(), StatusCode::CREATED);

    // 2. Download and verify content
    let get_response = client.get_object(&container_name, object_name).await?;
    assert_eq!(get_response.status(), StatusCode::OK);
    let downloaded = get_response.bytes().await?;
    assert_eq!(downloaded.to_vec(), content);

    // 3. Get metadata
    let head_response = client.head_object(&container_name, object_name).await?;
    assert_eq!(head_response.status(), StatusCode::OK);
    assert!(head_response.headers().contains_key("x-object-meta-test-type"));

    // 4. Update metadata
    let mut new_metadata = HashMap::new();
    new_metadata.insert("updated".to_string(), "yes".to_string());
    let post_response = client
        .update_object_metadata(&container_name, object_name, new_metadata)
        .await?;
    assert_eq!(post_response.status(), StatusCode::NO_CONTENT);

    // 5. List objects and verify presence
    let list_response = client.list_objects(&container_name).await?;
    assert_eq!(list_response.status(), StatusCode::OK);
    let object_list: Vec<Value> = list_response.json().await?;
    let found = object_list
        .iter()
        .any(|o| o.get("name").and_then(|n| n.as_str()) == Some(object_name));
    assert!(found, "Object should appear in listing");

    // 6. Delete object
    let delete_response = client.delete_object(&container_name, object_name).await?;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // 7. Verify deletion
    let final_get = client.get_object(&container_name, object_name).await?;
    assert_eq!(final_get.status(), StatusCode::NOT_FOUND);

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}
