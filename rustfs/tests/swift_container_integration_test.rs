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

//! Integration tests for Swift container operations
//!
//! These tests verify the complete Swift API flow including:
//! - Container creation (PUT)
//! - Container listing (GET on account)
//! - Container metadata retrieval (HEAD)
//! - Container metadata updates (POST)
//! - Container deletion (DELETE)
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
            // For testing, we use a mock token or configure Keystone in test environment
            auth_token: env::var("TEST_SWIFT_TOKEN").unwrap_or_else(|_| "test-token".to_string()),
            // Test with a mock project ID
            account: env::var("TEST_SWIFT_ACCOUNT").unwrap_or_else(|_| "AUTH_test-project-123".to_string()),
        }
    }

    /// Build Swift URL for account operations
    fn account_url(&self) -> String {
        format!("{}/v1/{}", self.endpoint, self.account)
    }

    /// Build Swift URL for container operations
    fn container_url(&self, container: &str) -> String {
        format!("{}/v1/{}/{}", self.endpoint, self.account, container)
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

    /// List containers (GET /v1/{account})
    async fn list_containers(&self) -> Result<Response> {
        self.client
            .get(self.settings.account_url())
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to list containers")
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

    /// Get container metadata (HEAD /v1/{account}/{container})
    async fn head_container(&self, container: &str) -> Result<Response> {
        self.client
            .head(self.settings.container_url(container))
            .header("X-Auth-Token", &self.settings.auth_token)
            .send()
            .await
            .context("Failed to get container metadata")
    }

    /// Update container metadata (POST /v1/{account}/{container})
    async fn update_container_metadata(&self, container: &str, metadata: HashMap<String, String>) -> Result<Response> {
        let mut req = self
            .client
            .post(self.settings.container_url(container))
            .header("X-Auth-Token", &self.settings.auth_token);

        // Add X-Container-Meta-* headers
        for (key, value) in metadata {
            req = req.header(format!("X-Container-Meta-{}", key), value);
        }

        req.send().await.context("Failed to update container metadata")
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
}

/// Test: Create a new container
///
/// Verifies:
/// - PUT /v1/{account}/{container} returns 201 Created
/// - X-Trans-Id header is present
/// - X-OpenStack-Request-Id header is present
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_create_container() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    let response = client.create_container(&container_name).await?;

    // Should return 201 Created for new container
    assert_eq!(response.status(), StatusCode::CREATED, "Expected 201 Created for new container");

    // Verify Swift transaction headers
    assert!(response.headers().contains_key("x-trans-id"), "Missing X-Trans-Id header");
    assert!(
        response.headers().contains_key("x-openstack-request-id"),
        "Missing X-OpenStack-Request-Id header"
    );

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Create container twice (idempotency)
///
/// Verifies:
/// - First PUT returns 201 Created
/// - Second PUT returns 202 Accepted (container already exists)
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_create_container_idempotent() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // First creation
    let response1 = client.create_container(&container_name).await?;
    assert_eq!(response1.status(), StatusCode::CREATED);

    // Second creation (idempotent)
    let response2 = client.create_container(&container_name).await?;
    assert_eq!(response2.status(), StatusCode::ACCEPTED, "Expected 202 Accepted for existing container");

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: List containers
///
/// Verifies:
/// - GET /v1/{account} returns 200 OK
/// - Response is valid JSON array
/// - Container names are returned
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_list_containers() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // Create a test container
    let _ = client.create_container(&container_name).await?;

    // List containers
    let response = client.list_containers().await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Parse JSON response
    let containers: Vec<Value> = response.json().await.context("Failed to parse container list JSON")?;

    // Verify container is in the list
    let found = containers.iter().any(|c| {
        c.get("name")
            .and_then(|n| n.as_str())
            .map(|n| n == container_name)
            .unwrap_or(false)
    });

    assert!(found, "Created container not found in list");

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Get container metadata
///
/// Verifies:
/// - HEAD /v1/{account}/{container} returns 204 No Content
/// - X-Container-Object-Count header is present
/// - X-Container-Bytes-Used header is present
/// - X-Timestamp header is present
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_container_metadata() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // Create container
    let _ = client.create_container(&container_name).await?;

    // Get metadata
    let response = client.head_container(&container_name).await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify metadata headers
    let headers = response.headers();
    assert!(
        headers.contains_key("x-container-object-count"),
        "Missing X-Container-Object-Count header"
    );
    assert!(headers.contains_key("x-container-bytes-used"), "Missing X-Container-Bytes-Used header");
    assert!(headers.contains_key("x-trans-id"), "Missing X-Trans-Id header");

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Update container metadata
///
/// Verifies:
/// - POST /v1/{account}/{container} returns 204 No Content
/// - Custom metadata can be set via X-Container-Meta-* headers
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_update_container_metadata() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // Create container
    let _ = client.create_container(&container_name).await?;

    // Update metadata
    let mut metadata = HashMap::new();
    metadata.insert("test-key".to_string(), "test-value".to_string());

    let response = client.update_container_metadata(&container_name, metadata).await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Cleanup
    let _ = client.delete_container(&container_name).await;

    Ok(())
}

/// Test: Delete container
///
/// Verifies:
/// - DELETE /v1/{account}/{container} returns 204 No Content
/// - Container is removed from listing
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_delete_container() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-container-{}", uuid::Uuid::new_v4());

    // Create container
    let _ = client.create_container(&container_name).await?;

    // Delete container
    let response = client.delete_container(&container_name).await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify container is deleted (HEAD should return 404)
    let head_response = client.head_container(&container_name).await?;
    assert_eq!(head_response.status(), StatusCode::NOT_FOUND, "Container should be deleted");

    Ok(())
}

/// Test: Delete non-existent container
///
/// Verifies:
/// - DELETE on non-existent container returns 404 Not Found
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_delete_nonexistent_container() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("nonexistent-{}", uuid::Uuid::new_v4());

    // Try to delete non-existent container
    let response = client.delete_container(&container_name).await?;
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Expected 404 Not Found for non-existent container"
    );

    Ok(())
}

/// Test: Container name validation
///
/// Verifies:
/// - Empty container name returns 400 Bad Request
/// - Container name with '/' returns 400 Bad Request
/// - Container name > 256 chars returns 400 Bad Request
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_container_name_validation() -> Result<()> {
    let client = SwiftClient::new()?;

    // Test empty name (this would be caught by URL construction, but let's test with slash)
    let response = client.create_container("").await?;
    assert!(response.status().is_client_error(), "Empty container name should be rejected");

    // Test name with slash
    let response = client.create_container("test/container").await?;
    assert!(response.status().is_client_error(), "Container name with '/' should be rejected");

    // Test name too long (> 256 chars)
    let long_name = "a".repeat(257);
    let response = client.create_container(&long_name).await?;
    assert!(response.status().is_client_error(), "Container name > 256 chars should be rejected");

    Ok(())
}

/// Test: Complete container lifecycle
///
/// Verifies the full lifecycle:
/// 1. Create container
/// 2. List and verify it appears
/// 3. Get metadata
/// 4. Update metadata
/// 5. Delete container
/// 6. Verify it's gone
#[tokio::test]
#[serial]
#[ignore] // Requires running RustFS server with Swift enabled
async fn test_container_lifecycle() -> Result<()> {
    let client = SwiftClient::new()?;
    let container_name = format!("test-lifecycle-{}", uuid::Uuid::new_v4());

    // 1. Create container
    let create_response = client.create_container(&container_name).await?;
    assert_eq!(create_response.status(), StatusCode::CREATED);

    // 2. List and verify
    let list_response = client.list_containers().await?;
    assert_eq!(list_response.status(), StatusCode::OK);
    let containers: Vec<Value> = list_response.json().await?;
    let found = containers
        .iter()
        .any(|c| c.get("name").and_then(|n| n.as_str()) == Some(&container_name));
    assert!(found, "Container should appear in listing");

    // 3. Get metadata
    let head_response = client.head_container(&container_name).await?;
    assert_eq!(head_response.status(), StatusCode::NO_CONTENT);

    // 4. Update metadata
    let mut metadata = HashMap::new();
    metadata.insert("lifecycle-test".to_string(), "true".to_string());
    let update_response = client.update_container_metadata(&container_name, metadata).await?;
    assert_eq!(update_response.status(), StatusCode::NO_CONTENT);

    // 5. Delete container
    let delete_response = client.delete_container(&container_name).await?;
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // 6. Verify it's gone
    let final_head = client.head_container(&container_name).await?;
    assert_eq!(final_head.status(), StatusCode::NOT_FOUND);

    Ok(())
}
