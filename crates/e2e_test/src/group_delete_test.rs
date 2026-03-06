// Copyright 2026 RustFS Team
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

//! E2E tests for group management (fixes #2028).

use crate::common::{RustFSTestEnvironment, awscurl_delete, awscurl_get, awscurl_put, init_logging};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::{Client, Config};
use serial_test::serial;
use tracing::info;

fn create_user_s3_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "e2e-group-test");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

/// Test that deleting a group with members fails, and deleting an empty group succeeds.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires awscurl and spawns a real RustFS server"]
async fn test_delete_group_requires_empty_membership() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    // 1. Create a user
    let add_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey=testuser1", env.url);
    let user_body = serde_json::json!({
        "secretKey": "testuser1secret",
        "status": "enabled"
    });
    awscurl_put(&add_user_url, &user_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Created testuser1");

    // 2. Create a group with testuser1 as a member
    let update_members_url = format!("{}/rustfs/admin/v3/update-group-members", env.url);
    let add_member_body = serde_json::json!({
        "group": "testgroup",
        "members": ["testuser1"],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    awscurl_put(&update_members_url, &add_member_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Added testuser1 to testgroup");

    // 3. Attempt to delete the group while it still has members — should fail
    let delete_group_url = format!("{}/rustfs/admin/v3/group/testgroup", env.url);
    let delete_result = awscurl_delete(&delete_group_url, &env.access_key, &env.secret_key).await;
    assert!(delete_result.is_err(), "deleting a non-empty group should fail");
    info!("Delete of non-empty group correctly rejected");

    // 4. Remove the member from the group
    let remove_member_body = serde_json::json!({
        "group": "testgroup",
        "members": ["testuser1"],
        "isRemove": true,
        "groupStatus": "enabled"
    });
    awscurl_put(&update_members_url, &remove_member_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Removed testuser1 from testgroup");

    // 5. Delete the now-empty group — should succeed
    awscurl_delete(&delete_group_url, &env.access_key, &env.secret_key).await?;
    info!("Deleted empty testgroup successfully");

    // 6. Verify the group no longer exists
    let get_group_url = format!("{}/rustfs/admin/v3/group?group=testgroup", env.url);
    let get_result = awscurl_get(&get_group_url, &env.access_key, &env.secret_key).await;
    assert!(get_result.is_err(), "group should no longer exist after deletion");
    info!("Confirmed testgroup no longer exists");

    Ok(())
}

/// Test that a user with only group membership (no explicit user policy) gets group policies
/// and can perform actions allowed by the group (regression test for #2028.1).
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires awscurl and spawns a real RustFS server"]
async fn test_user_with_only_group_gets_group_policies() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let user_name = "grouponlyuser";
    let user_secret = "grouponlysecret";
    let group_name = "policygroup";
    let policy_name = "ListBucketsOnlyPolicy";

    // 1. Create canned policy that allows ListAllMyBuckets only
    let policy_doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListAllMyBuckets"],
            "Resource": ["*"]
        }]
    });
    let add_policy_url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    awscurl_put(&add_policy_url, &policy_doc.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Created canned policy {}", policy_name);

    // 2. Create user with no explicit policy
    let add_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, user_name);
    let user_body = serde_json::json!({
        "secretKey": user_secret,
        "status": "enabled"
    });
    awscurl_put(&add_user_url, &user_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Created user {} with no explicit policy", user_name);

    // 3. Add user to group (creates group with this member; user_group_memberships must be updated)
    let update_members_url = format!("{}/rustfs/admin/v3/update-group-members", env.url);
    let add_member_body = serde_json::json!({
        "group": group_name,
        "members": [user_name],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    awscurl_put(&update_members_url, &add_member_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Added {} to group {}", user_name, group_name);

    // 4. Attach policy to group
    let set_policy_url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=true",
        env.url, policy_name, group_name
    );
    awscurl_put(&set_policy_url, "", &env.access_key, &env.secret_key).await?;
    info!("Attached policy {} to group {}", policy_name, group_name);

    // 5. User with only group (no user policy) should be able to list buckets
    let user_client = create_user_s3_client(&env, user_name, user_secret);
    let list_result = user_client.list_buckets().send().await;
    list_result.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        format!("User with only group membership should get group policies and list buckets: {}", e).into()
    })?;
    info!("User with only group successfully listed buckets (group policies applied)");

    Ok(())
}

/// Test that after deleting a user who was the only member of a group, the group can be deleted
/// (regression test for #2028.2: delete group uses backend membership, not stale cache).
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires awscurl and spawns a real RustFS server"]
async fn test_delete_group_after_deleting_user() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let user_name = "solemember";
    let user_secret = "solemembersecret";
    let group_name = "soledeletegroup";

    // 1. Create user
    let add_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, user_name);
    let user_body = serde_json::json!({
        "secretKey": user_secret,
        "status": "enabled"
    });
    awscurl_put(&add_user_url, &user_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Created user {}", user_name);

    // 2. Add user to group
    let update_members_url = format!("{}/rustfs/admin/v3/update-group-members", env.url);
    let add_member_body = serde_json::json!({
        "group": group_name,
        "members": [user_name],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    awscurl_put(&update_members_url, &add_member_body.to_string(), &env.access_key, &env.secret_key).await?;
    info!("Added {} to group {}", user_name, group_name);

    // 3. Delete the user (backend and cache update so group membership becomes empty)
    let remove_user_url = format!("{}/rustfs/admin/v3/remove-user?accessKey={}", env.url, user_name);
    awscurl_delete(&remove_user_url, &env.access_key, &env.secret_key).await?;
    info!("Deleted user {}", user_name);

    // 4. Deleting the group should succeed (backend has empty members; no stale cache)
    let delete_group_url = format!("{}/rustfs/admin/v3/group/{}", env.url, group_name);
    awscurl_delete(&delete_group_url, &env.access_key, &env.secret_key).await?;
    info!("Deleted group {} after user was removed", group_name);

    Ok(())
}
