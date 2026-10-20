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

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
    init_logging, signed_request,
};
use aws_sdk_s3::primitives::ByteStream;
use http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn set_policy(env: &RustFSTestEnvironment, user: &str, policy: Value) -> TestResult {
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &env.url,
        &env.access_key,
        &env.secret_key,
        user,
        &policy.to_string(),
    )
    .await?;
    admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, user, user).await
}

async fn catalog_request(
    env: &RustFSTestEnvironment,
    credentials: &(String, String),
    method: Method,
    path: &str,
    body: Option<Value>,
    expected: StatusCode,
) -> TestResult<Value> {
    let response = signed_request(
        method.clone(),
        &format!("{}{path}", env.url),
        &credentials.0,
        &credentials.1,
        body.map(|value| serde_json::to_vec(&value)).transpose()?,
        Some("application/json"),
    )
    .await?;
    let status = response.status();
    let text = response.text().await?;
    assert_eq!(status, expected, "{method} {path}: {text}");
    if text.is_empty() || !status.is_success() {
        Ok(Value::Null)
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

async fn get_object(
    env: &RustFSTestEnvironment,
    credentials: &(String, String),
    bucket: &str,
    key: &str,
    expected: StatusCode,
) -> TestResult {
    let response = signed_request(
        Method::GET,
        &format!("{}/{bucket}/{key}", env.url),
        &credentials.0,
        &credentials.1,
        None,
        None,
    )
    .await?;
    assert_eq!(response.status(), expected, "GET {bucket}/{key}");
    if expected == StatusCode::OK {
        assert_eq!(response.bytes().await?.as_ref(), b"original object");
    }
    Ok(())
}

fn create_table(name: &str) -> Value {
    json!({
        "name": name,
        "schema": {
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]
        },
        "stage-create": false
    })
}

fn staged_commit(metadata: &Value, location: &str) -> Value {
    json!({
        "requirements": [{"type": "assert-create"}],
        "updates": [
            {"action": "assign-uuid", "uuid": metadata["table-uuid"]},
            {"action": "upgrade-format-version", "format-version": metadata["format-version"]},
            {"action": "add-schema", "schema": metadata["schemas"][0]},
            {"action": "set-current-schema", "schema-id": -1},
            {"action": "add-spec", "spec": metadata["partition-specs"][0]},
            {"action": "set-default-spec", "spec-id": -1},
            {"action": "add-sort-order", "sort-order": metadata["sort-orders"][0]},
            {"action": "set-default-sort-order", "sort-order-id": -1},
            {"action": "set-location", "location": location}
        ]
    })
}

async fn location_claim_preserves_other_users_objects_after_drop_and_restart(backing: &str) -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = [("RUSTFS_TABLE_CATALOG_BACKING", backing)];
    env.start_rustfs_server_without_cleanup_with_env(&[("RUSTFS_TABLE_CATALOG_BACKING", "object")])
        .await?;
    env.wait_for_server_ready().await?;
    let root = (env.access_key.clone(), env.secret_key.clone());
    let creator = (format!("creator-{}", Uuid::new_v4().simple()), Uuid::new_v4().simple().to_string());
    let reader = (format!("reader-{}", Uuid::new_v4().simple()), Uuid::new_v4().simple().to_string());
    let bucket = "table-location-authorization";
    let root_s3 = env.create_s3_client();
    root_s3.create_bucket().bucket(bucket).send().await?;
    for key in ["shared/known.txt", "shared2/known.txt", "authorized/known.txt"] {
        root_s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"original object"))
            .send()
            .await?;
    }
    let catalog = format!("/iceberg/v1/{bucket}/namespaces/lab/tables");
    catalog_request(&env, &root, Method::PUT, &format!("/iceberg/v1/buckets/{bucket}"), None, StatusCode::OK).await?;
    catalog_request(
        &env,
        &root,
        Method::POST,
        &format!("/iceberg/v1/{bucket}/namespaces"),
        Some(json!({"namespace": ["lab"]})),
        StatusCode::OK,
    )
    .await?;
    if backing == "durable-strong" {
        catalog_request(
            &env,
            &root,
            Method::POST,
            &format!("/iceberg/v1/{bucket}/catalog/warehouse-index/backfill"),
            None,
            StatusCode::NO_CONTENT,
        )
        .await?;
        let preflight = catalog_request(
            &env,
            &root,
            Method::GET,
            &format!("/iceberg/v1/{bucket}/catalog/migration"),
            None,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(preflight["blockers"], json!([]), "{preflight}");
        let migration = catalog_request(
            &env,
            &root,
            Method::POST,
            &format!("/iceberg/v1/{bucket}/catalog/migration"),
            None,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(migration["ready_to_enable_durable_strong"], json!(true), "{migration}");
        env.restart_server_preserving_data(vec![], &server_env).await?;
        env.wait_for_server_ready().await?;
    }
    admin_create_user(&env, &creator.0, &creator.1).await?;
    admin_create_user(&env, &reader.0, &reader.1).await?;
    let create_statement = json!({
        "Effect": "Allow",
        "Action": "admin:CreateTable",
        "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab")
    });
    set_policy(
        &env,
        &creator.0,
        json!({"Version": "2012-10-17", "Statement": [create_statement.clone()]}),
    )
    .await?;
    set_policy(
        &env,
        &reader.0,
        json!({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "s3:GetObject", "Resource": format!("arn:aws:s3:::{bucket}/*")
        }]}),
    )
    .await?;
    for key in ["shared/known.txt", "shared2/known.txt"] {
        get_object(&env, &reader, bucket, key, StatusCode::OK).await?;
        get_object(&env, &creator, bucket, key, StatusCode::FORBIDDEN).await?;
    }
    for (method, path) in [
        (Method::PUT, format!("/{bucket}/shared/new.txt")),
        (Method::GET, format!("/{bucket}?list-type=2")),
    ] {
        let response = signed_request(method, &format!("{}{path}", env.url), &creator.0, &creator.1, None, None).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    // Both direct and staged requests must reject a caller-selected foreign prefix.
    for staged in [false, true] {
        for location in [format!("s3://{bucket}/shared"), format!("s3://{bucket}/shared/")] {
            let mut request = create_table("claim");
            request["location"] = json!(location);
            request["stage-create"] = json!(staged);
            catalog_request(&env, &creator, Method::POST, &catalog, Some(request), StatusCode::FORBIDDEN).await?;
        }
    }
    catalog_request(&env, &root, Method::GET, &format!("{catalog}/claim"), None, StatusCode::NOT_FOUND).await?;

    // Server-assigned locations still need only CreateTable, and retain protection after drop.
    let created = catalog_request(&env, &creator, Method::POST, &catalog, Some(create_table("managed")), StatusCode::OK).await?;
    let location = created["metadata"]["location"].as_str().ok_or("missing table location")?;
    let managed_key = format!(
        "{}/known.txt",
        location
            .strip_prefix(&format!("s3://{bucket}/"))
            .ok_or("table location has wrong bucket")?
    );
    root_s3
        .put_object()
        .bucket(bucket)
        .key(&managed_key)
        .body(ByteStream::from_static(b"original object"))
        .send()
        .await?;
    get_object(&env, &reader, bucket, &managed_key, StatusCode::FORBIDDEN).await?;
    catalog_request(&env, &root, Method::DELETE, &format!("{catalog}/managed"), None, StatusCode::NO_CONTENT).await?;
    env.restart_server_preserving_data(vec![], &server_env).await?;
    env.wait_for_server_ready().await?;
    for table in ["claim", "managed"] {
        catalog_request(&env, &root, Method::GET, &format!("{catalog}/{table}"), None, StatusCode::NOT_FOUND).await?;
    }
    for key in ["shared/known.txt", "shared2/known.txt", "authorized/known.txt"] {
        get_object(&env, &reader, bucket, key, StatusCode::OK).await?;
    }
    get_object(&env, &reader, bucket, &managed_key, StatusCode::FORBIDDEN).await?;
    get_object(&env, &root, bucket, &managed_key, StatusCode::OK).await?;

    // Explicit adoption is scoped to a physical prefix and does not grant S3 access.
    set_policy(
        &env,
        &creator.0,
        json!({"Version": "2012-10-17", "Statement": [
            create_statement.clone(),
            {"Effect": "Allow", "Action": "admin:ClaimTableLocation", "Resource": format!("arn:aws:s3:::{bucket}/authorized")}
        ]}),
    )
    .await?;
    let mut request = create_table("authorized");
    request["location"] = json!(format!("s3://{bucket}/shared"));
    catalog_request(&env, &creator, Method::POST, &catalog, Some(request.clone()), StatusCode::FORBIDDEN).await?;
    request["location"] = json!(format!("s3://{bucket}/authorized"));
    let adopted = catalog_request(&env, &creator, Method::POST, &catalog, Some(request), StatusCode::OK).await?;
    get_object(&env, &creator, bucket, "authorized/known.txt", StatusCode::FORBIDDEN).await?;
    get_object(&env, &reader, bucket, "authorized/known.txt", StatusCode::FORBIDDEN).await?;
    // Existing updates retain their permissions; relocation and re-registration
    // require a prefix grant even when the caller can access every S3 object.
    set_policy(
        &env,
        &creator.0,
        json!({"Version": "2012-10-17", "Statement": [
            create_statement,
            {"Effect": "Allow", "Action": "admin:RegisterTable", "Resource": [
                format!("arn:aws:s3:::{bucket}/namespaces/lab"),
                format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/*")
            ]},
            {"Effect": "Allow", "Action": "admin:CommitTable", "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/*")},
            {"Effect": "Allow", "Action": "admin:SetTableMetadataLocation", "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/authorized")},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": format!("arn:aws:s3:::{bucket}/*")}
        ]}),
    )
    .await?;
    let requirements = json!([{"type": "assert-table-uuid", "uuid": adopted["metadata"]["table-uuid"]}]);
    let committed = catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("{catalog}/authorized"),
        Some(json!({
            "requirements": requirements,
            "updates": [{"action": "set-properties", "updates": {"test.location-authorization": "retained"}}]
        })),
        StatusCode::OK,
    )
    .await?;
    catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("{catalog}/authorized"),
        Some(json!({
            "requirements": requirements,
            "updates": [{"action": "set-location", "location": format!("s3://{bucket}/shared")}]
        })),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let current = catalog_request(&env, &root, Method::GET, &format!("{catalog}/authorized"), None, StatusCode::OK).await?;
    assert_eq!(current["metadata"]["location"], json!(format!("s3://{bucket}/authorized")));
    assert_eq!(current["metadata-location"], committed["metadata-location"]);
    catalog_request(
        &env,
        &root,
        Method::DELETE,
        &format!("{catalog}/authorized"),
        None,
        StatusCode::NO_CONTENT,
    )
    .await?;
    catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("/iceberg/v1/{bucket}/namespaces/lab/register"),
        Some(json!({"name": "authorized", "metadata-location": committed["metadata-location"]})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("{catalog}/authorized/catalog/import"),
        Some(json!({"metadata-location": committed["metadata-location"]})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    if backing == "object" {
        catalog_request(
            &env,
            &creator,
            Method::POST,
            &format!("{catalog}/authorized/catalog/external/sync"),
            Some(json!({
                "catalog": "polaris",
                "external-namespace": "lab",
                "external-table": "authorized",
                "metadata-location": committed["metadata-location"]
            })),
            StatusCode::FORBIDDEN,
        )
        .await?;
    }

    // Staged metadata is client-controlled and must be authorized at publication.
    let mut request = create_table("staged");
    request["stage-create"] = json!(true);
    let staged = catalog_request(&env, &creator, Method::POST, &catalog, Some(request), StatusCode::OK).await?;
    catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("{catalog}/staged"),
        Some(staged_commit(&staged["metadata"], &format!("s3://{bucket}/shared"))),
        StatusCode::FORBIDDEN,
    )
    .await?;
    catalog_request(&env, &root, Method::GET, &format!("{catalog}/staged"), None, StatusCode::NOT_FOUND).await?;
    set_policy(
        &env,
        &creator.0,
        json!({"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "admin:CreateTable", "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab")},
            {"Effect": "Allow", "Action": "admin:CommitTable", "Resource": format!("arn:aws:s3:::{bucket}/namespaces/lab/tables/staged")},
            {"Effect": "Allow", "Action": "admin:ClaimTableLocation", "Resource": format!("arn:aws:s3:::{bucket}/stage-owned")},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": format!("arn:aws:s3:::{bucket}/*")}
        ]}),
    )
    .await?;
    catalog_request(
        &env,
        &creator,
        Method::POST,
        &format!("{catalog}/staged"),
        Some(staged_commit(&staged["metadata"], &format!("s3://{bucket}/stage-owned"))),
        StatusCode::OK,
    )
    .await?;
    catalog_request(&env, &root, Method::DELETE, &format!("{catalog}/staged"), None, StatusCode::NO_CONTENT).await?;
    env.restart_server_preserving_data(vec![], &server_env).await?;
    env.wait_for_server_ready().await?;
    for table in ["authorized", "staged"] {
        catalog_request(&env, &root, Method::GET, &format!("{catalog}/{table}"), None, StatusCode::NOT_FOUND).await?;
    }
    get_object(&env, &reader, bucket, "authorized/known.txt", StatusCode::FORBIDDEN).await?;
    get_object(&env, &root, bucket, "authorized/known.txt", StatusCode::OK).await?;
    for key in ["shared/known.txt", "shared2/known.txt"] {
        get_object(&env, &reader, bucket, key, StatusCode::OK).await?;
    }
    Ok(())
}

#[tokio::test]
async fn object_catalog_location_claim_preserves_objects_after_drop_and_restart() -> TestResult {
    location_claim_preserves_other_users_objects_after_drop_and_restart("object").await
}

#[tokio::test]
async fn durable_strong_catalog_location_claim_preserves_objects_after_drop_and_restart() -> TestResult {
    location_claim_preserves_other_users_objects_after_drop_and_restart("durable-strong").await
}
