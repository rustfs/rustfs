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

use crate::common::{RustFSTestEnvironment, admin_ok, admin_request, init_logging};
use aws_sdk_s3::Client;
use tracing::info;

fn create_user_s3_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    env.create_s3_client_with_credentials(access_key, secret_key)
}

#[tokio::test(flavor = "multi_thread")]
async fn update_group_members_rejects_invalid_new_group_names() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let invalid_groups = [
        ("test group", "group name contains whitespace"),
        ("test=group", "group name contains reserved characters =,"),
        ("test,group", "group name contains reserved characters =,"),
    ];

    for (group, expected_message) in invalid_groups {
        let body = serde_json::json!({
            "group": group,
            "members": [],
            "isRemove": false,
            "groupStatus": "enabled"
        })
        .to_string();
        let (status, response_body) = admin_request(
            &env.url,
            http::Method::PUT,
            "/rustfs/admin/v3/update-group-members",
            Some(body),
            &env.access_key,
            &env.secret_key,
        )
        .await?;

        assert_eq!(
            status,
            reqwest::StatusCode::BAD_REQUEST,
            "invalid group {group:?} must return HTTP 400, body: {response_body}"
        );
        assert!(
            response_body.contains("<Code>InvalidArgument</Code>"),
            "invalid group {group:?} must return InvalidArgument, body: {response_body}"
        );
        assert!(
            response_body.contains(&format!("<Message>{expected_message}</Message>")),
            "invalid group {group:?} returned an unexpected message: {response_body}"
        );
    }

    env.stop_server();
    Ok(())
}

/// Test that deleting a group with members fails, and deleting an empty group succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_group_requires_empty_membership() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    // 1. Create a user
    let user_body = serde_json::json!({
        "secretKey": "testuser1secret",
        "status": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/add-user?accessKey=testuser1",
        Some(user_body.to_string()),
    )
    .await?;
    info!("Created testuser1");

    // 2. Create a group with testuser1 as a member
    let add_member_body = serde_json::json!({
        "group": "testgroup",
        "members": ["testuser1"],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/update-group-members",
        Some(add_member_body.to_string()),
    )
    .await?;
    info!("Added testuser1 to testgroup");

    // 3. Attempt to delete the group while it still has members — should fail
    let (delete_status, delete_body) = admin_request(
        &env.url,
        http::Method::DELETE,
        "/rustfs/admin/v3/group/testgroup",
        None,
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert_eq!(
        delete_status,
        reqwest::StatusCode::BAD_REQUEST,
        "deleting a non-empty group must return HTTP 400, body: {delete_body}"
    );
    assert!(
        delete_body.contains("<Code>InvalidRequest</Code>"),
        "deleting a non-empty group must return InvalidRequest, body: {delete_body}"
    );
    assert!(
        delete_body.contains("<Message>group is not empty</Message>"),
        "deleting a non-empty group returned an unexpected message: {delete_body}"
    );
    info!("Delete of non-empty group correctly rejected");

    // 4. Remove the member from the group
    let remove_member_body = serde_json::json!({
        "group": "testgroup",
        "members": ["testuser1"],
        "isRemove": true,
        "groupStatus": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/update-group-members",
        Some(remove_member_body.to_string()),
    )
    .await?;
    info!("Removed testuser1 from testgroup");

    // 5. Delete the now-empty group — should succeed
    admin_ok(&env, http::Method::DELETE, "/rustfs/admin/v3/group/testgroup", None).await?;
    info!("Deleted empty testgroup successfully");

    // 6. Verify the group no longer exists
    let (get_status, get_body) = admin_request(
        &env.url,
        http::Method::GET,
        "/rustfs/admin/v3/group?group=testgroup",
        None,
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert_eq!(
        get_status,
        reqwest::StatusCode::NOT_FOUND,
        "a deleted group must return HTTP 404, body: {get_body}"
    );
    assert!(
        get_body.contains("<Code>NoSuchResource</Code>"),
        "a deleted group must return NoSuchResource, body: {get_body}"
    );
    assert!(
        get_body.contains("<Message>group &apos;testgroup&apos; does not exist</Message>"),
        "a deleted group returned an unexpected message: {get_body}"
    );
    info!("Confirmed testgroup no longer exists");

    Ok(())
}

/// Test that a user with only group membership (no explicit user policy) gets group policies
/// and can perform actions allowed by the group (regression test for #2028.1).
#[tokio::test(flavor = "multi_thread")]
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
            "Resource": ["arn:aws:s3:::*"]
        }]
    });
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy_name}"),
        Some(policy_doc.to_string()),
    )
    .await?;
    info!("Created canned policy {}", policy_name);

    // 2. Create user with no explicit policy
    let user_body = serde_json::json!({
        "secretKey": user_secret,
        "status": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user_name}"),
        Some(user_body.to_string()),
    )
    .await?;
    info!("Created user {} with no explicit policy", user_name);

    // 3. Add user to group (creates group with this member; user_group_memberships must be updated)
    let add_member_body = serde_json::json!({
        "group": group_name,
        "members": [user_name],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/update-group-members",
        Some(add_member_body.to_string()),
    )
    .await?;
    info!("Added {} to group {}", user_name, group_name);

    // 4. Attach policy to group
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-user-or-group-policy?policyName={policy_name}&userOrGroup={group_name}&isGroup=true"),
        Some(String::new()),
    )
    .await?;
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
async fn test_delete_group_after_deleting_user() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let user_name = "solemember";
    let user_secret = "solemembersecret";
    let group_name = "soledeletegroup";

    // 1. Create user
    let user_body = serde_json::json!({
        "secretKey": user_secret,
        "status": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user_name}"),
        Some(user_body.to_string()),
    )
    .await?;
    info!("Created user {}", user_name);

    // 2. Add user to group
    let add_member_body = serde_json::json!({
        "group": group_name,
        "members": [user_name],
        "isRemove": false,
        "groupStatus": "enabled"
    });
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/update-group-members",
        Some(add_member_body.to_string()),
    )
    .await?;
    info!("Added {} to group {}", user_name, group_name);

    // 3. Delete the user (backend and cache update so group membership becomes empty)
    admin_ok(
        &env,
        http::Method::DELETE,
        &format!("/rustfs/admin/v3/remove-user?accessKey={user_name}"),
        None,
    )
    .await?;
    info!("Deleted user {}", user_name);

    // 4. Deleting the group should succeed (backend has empty members; no stale cache)
    admin_ok(&env, http::Method::DELETE, &format!("/rustfs/admin/v3/group/{group_name}"), None).await?;
    info!("Deleted group {} after user was removed", group_name);

    Ok(())
}
