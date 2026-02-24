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

use crate::common::{RustFSTestEnvironment, awscurl_delete, awscurl_get, awscurl_put, init_logging};
use serial_test::serial;
use tracing::info;

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
