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

use rustfs_credentials::Credentials;
use rustfs_keystone::middleware::KEYSTONE_CREDENTIALS;
use rustfs_keystone::{KeystoneAuthLayer, KeystoneAuthProvider, KeystoneClient, KeystoneVersion};
use std::collections::HashMap;
use std::sync::Arc;

/// Create a KeystoneAuthProvider for testing (no actual Keystone connection)
fn create_test_auth_provider() -> Arc<KeystoneAuthProvider> {
    let client = KeystoneClient::new(
        "http://localhost:5000".to_string(),
        KeystoneVersion::V3,
        Some("admin".to_string()),
        Some("secret".to_string()),
        Some("admin".to_string()),
        "Default".to_string(),
        false, // Don't verify SSL for tests
    );

    Arc::new(KeystoneAuthProvider::new(client, 1000, std::time::Duration::from_secs(300), true))
}

/// Helper to create test credentials
fn create_test_credentials(access_key: &str, parent_user: &str) -> Credentials {
    Credentials {
        access_key: access_key.to_string(),
        secret_key: String::new(),
        session_token: String::new(),
        expiration: None,
        status: "Active".to_string(),
        parent_user: parent_user.to_string(),
        groups: None,
        claims: None,
        name: None,
        description: None,
    }
}

#[test]
fn test_layer_creation_with_provider() {
    // Test that KeystoneAuthLayer can be created with an auth provider
    let auth_provider = create_test_auth_provider();
    let _layer = KeystoneAuthLayer::new(Some(auth_provider));

    // If this compiles and runs, the layer was created successfully
}

#[test]
fn test_layer_creation_without_provider() {
    // Test that KeystoneAuthLayer can be created without an auth provider (disabled mode)
    let _layer = KeystoneAuthLayer::new(None);

    // If this compiles and runs, the layer was created successfully
}

#[tokio::test]
async fn test_task_local_storage_scope() {
    // Test that task-local storage works correctly with scope
    let test_creds = {
        let mut claims = HashMap::new();
        claims.insert(
            "keystone".to_string(),
            serde_json::json!({
                "user_id": "test-user-id",
                "project_id": "test-project-id",
                "roles": ["member"]
            }),
        );

        Credentials {
            access_key: "keystone:test-user-id".to_string(),
            secret_key: String::new(),
            session_token: String::new(),
            expiration: None,
            status: "Active".to_string(),
            parent_user: "test-user".to_string(),
            groups: None,
            claims: Some(claims),
            name: None,
            description: None,
        }
    };

    // Test that credentials are available within scope
    let result = KEYSTONE_CREDENTIALS
        .scope(Some(test_creds.clone()), async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.clone())
                .unwrap_or(None)
        })
        .await;

    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert_eq!(retrieved.access_key, "keystone:test-user-id");
    assert_eq!(retrieved.parent_user, "test-user");
}

#[tokio::test]
async fn test_task_local_storage_isolation() {
    // Test that task-local storage is isolated between different async tasks
    let creds1 = create_test_credentials("keystone:user1", "user1");
    let creds2 = create_test_credentials("keystone:user2", "user2");

    // Spawn two tasks with different credentials
    let task1 = tokio::spawn(async move {
        KEYSTONE_CREDENTIALS
            .scope(Some(creds1), async {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                KEYSTONE_CREDENTIALS
                    .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                    .unwrap_or(None)
            })
            .await
    });

    let task2 = tokio::spawn(async move {
        KEYSTONE_CREDENTIALS
            .scope(Some(creds2), async {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                KEYSTONE_CREDENTIALS
                    .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                    .unwrap_or(None)
            })
            .await
    });

    // Verify each task got its own credentials
    let result1 = task1.await.unwrap();
    let result2 = task2.await.unwrap();

    assert_eq!(result1, Some("user1".to_string()));
    assert_eq!(result2, Some("user2".to_string()));
}

#[tokio::test]
async fn test_task_local_storage_none_scope() {
    // Test that scoping with None works correctly
    let result = KEYSTONE_CREDENTIALS
        .scope(None, async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.clone())
                .unwrap_or(None)
        })
        .await;

    assert!(result.is_none());
}

#[tokio::test]
async fn test_credentials_with_claims() {
    // Test that credentials with Keystone claims work correctly
    let mut claims = HashMap::new();
    claims.insert(
        "keystone".to_string(),
        serde_json::json!({
            "user_id": "test-user-id",
            "project_id": "test-project-id",
            "roles": ["admin", "member"]
        }),
    );

    let creds = Credentials {
        access_key: "keystone:test-user-id".to_string(),
        secret_key: String::new(),
        session_token: String::new(),
        expiration: None,
        status: "Active".to_string(),
        parent_user: "test-user".to_string(),
        groups: None,
        claims: Some(claims),
        name: None,
        description: None,
    };

    let result = KEYSTONE_CREDENTIALS
        .scope(Some(creds.clone()), async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.clone())
                .unwrap_or(None)
        })
        .await;

    assert!(result.is_some());
    let retrieved = result.unwrap();

    // Verify claims are preserved
    assert!(retrieved.claims.is_some());
    let claims_map = retrieved.claims.unwrap();
    assert!(claims_map.contains_key("keystone"));

    let keystone_claims = &claims_map["keystone"];
    assert_eq!(keystone_claims["user_id"], "test-user-id");
    assert_eq!(keystone_claims["project_id"], "test-project-id");

    // Verify roles
    let roles = keystone_claims["roles"].as_array().unwrap();
    assert_eq!(roles.len(), 2);
    assert!(roles.contains(&serde_json::json!("admin")));
    assert!(roles.contains(&serde_json::json!("member")));
}

#[tokio::test]
async fn test_nested_scopes() {
    // Test that nested scopes work correctly (inner scope takes precedence)
    let outer_creds = create_test_credentials("keystone:outer", "outer-user");
    let inner_creds = create_test_credentials("keystone:inner", "inner-user");

    let result = KEYSTONE_CREDENTIALS
        .scope(Some(outer_creds), async {
            // In outer scope
            let outer_result = KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                .unwrap_or(None);

            assert_eq!(outer_result, Some("outer-user".to_string()));

            // Enter inner scope
            KEYSTONE_CREDENTIALS
                .scope(Some(inner_creds), async {
                    let inner_result = KEYSTONE_CREDENTIALS
                        .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                        .unwrap_or(None);

                    assert_eq!(inner_result, Some("inner-user".to_string()));
                    inner_result
                })
                .await
        })
        .await;

    assert_eq!(result, Some("inner-user".to_string()));
}

#[test]
fn test_auth_provider_configuration() {
    // Test that AuthProvider can be configured with different settings
    let client = KeystoneClient::new(
        "http://keystone.example.com:5000".to_string(),
        KeystoneVersion::V3,
        Some("test-admin".to_string()),
        Some("test-password".to_string()),
        Some("test-project".to_string()),
        "TestDomain".to_string(),
        true,
    );

    // Test with caching enabled
    let provider1 = KeystoneAuthProvider::new(client.clone(), 5000, std::time::Duration::from_secs(600), true);

    // Verify provider was created (if this compiles, it worked)
    drop(provider1);

    // Test with caching disabled
    let provider2 = KeystoneAuthProvider::new(client, 0, std::time::Duration::from_secs(0), false);

    drop(provider2);
}

#[tokio::test]
async fn test_multiple_sequential_scopes() {
    // Test that multiple sequential scopes work correctly
    let creds1 = create_test_credentials("keystone:first", "first-user");
    let creds2 = create_test_credentials("keystone:second", "second-user");
    let creds3 = create_test_credentials("keystone:third", "third-user");

    // First scope
    let result1 = KEYSTONE_CREDENTIALS
        .scope(Some(creds1), async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                .unwrap_or(None)
        })
        .await;

    assert_eq!(result1, Some("first-user".to_string()));

    // Second scope
    let result2 = KEYSTONE_CREDENTIALS
        .scope(Some(creds2), async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                .unwrap_or(None)
        })
        .await;

    assert_eq!(result2, Some("second-user".to_string()));

    // Third scope
    let result3 = KEYSTONE_CREDENTIALS
        .scope(Some(creds3), async {
            KEYSTONE_CREDENTIALS
                .try_with(|c: &Option<Credentials>| c.as_ref().map(|cr| cr.parent_user.clone()))
                .unwrap_or(None)
        })
        .await;

    assert_eq!(result3, Some("third-user".to_string()));
}

#[tokio::test]
async fn test_task_local_outside_scope() {
    // Test that accessing task-local storage outside a scope returns None or error
    let result = KEYSTONE_CREDENTIALS
        .try_with(|c: &Option<Credentials>| c.clone())
        .ok()
        .flatten();

    // Outside any scope, should be None or error
    assert!(result.is_none());
}
