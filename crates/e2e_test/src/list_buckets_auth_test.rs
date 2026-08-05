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

//! Regression coverage for the MinIO-compatible filtered ListBuckets fallback.

use crate::common::{RustFSTestEnvironment, admin_ok, init_logging};
use std::error::Error;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn bucket_scoped_policy_returns_only_authorized_bucket() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let root_client = env.create_s3_client();
    let allowed_bucket = "list-buckets-authorized";
    let hidden_bucket = "list-buckets-hidden";
    let user = "listbucketsuser";
    let secret = "listbucketssecret";
    let policy = "list-buckets-scoped";

    root_client.create_bucket().bucket(allowed_bucket).send().await?;
    root_client.create_bucket().bucket(hidden_bucket).send().await?;

    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy}"),
        Some(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["s3:*"],
                    "Resource": [
                        format!("arn:aws:s3:::{allowed_bucket}"),
                        format!("arn:aws:s3:::{allowed_bucket}/*")
                    ]
                }]
            })
            .to_string(),
        ),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(serde_json::json!({ "secretKey": secret, "status": "enabled" }).to_string()),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::POST,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        Some(serde_json::json!({ "policies": [policy], "user": user }).to_string()),
    )
    .await?;

    let client = env.create_s3_client_with_credentials(user, secret);
    // Capture ListBuckets first so the direct-access control cannot warm bucket metadata and mask the regression.
    let listed = client.list_buckets().send().await;
    client.list_objects_v2().bucket(allowed_bucket).send().await?;

    let listed = listed?;
    let names = listed
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name().map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(names, vec![allowed_bucket]);

    Ok(())
}
