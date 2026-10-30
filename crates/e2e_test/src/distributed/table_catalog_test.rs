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

use super::harness::{DistCluster, DistLayout, TestResult, get_object_bytes, put_object, unique_bucket};
use crate::common::{init_logging, signed_request};
use http::{Method, StatusCode};
use serde_json::{Value, json};

async fn catalog_request(
    dist: &DistCluster,
    node: usize,
    method: Method,
    path: &str,
    body: Option<Value>,
    expected: StatusCode,
) -> TestResult<Value> {
    let response = signed_request(
        method.clone(),
        &format!("{}{path}", dist.cluster.nodes[node].url),
        &dist.cluster.access_key,
        &dist.cluster.secret_key,
        body.map(|value| serde_json::to_vec(&value)).transpose()?,
        Some("application/json"),
    )
    .await?;
    let status = response.status();
    let text = response.text().await?;
    assert_eq!(status, expected, "node {node}: {method} {path}: {text}");
    if text.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(serde_json::from_str(&text)?)
    }
}

async fn assert_bucket_state(dist: &DistCluster, bucket: &str, enabled: bool) -> TestResult {
    for node in 0..dist.cluster.nodes.len() {
        let response =
            catalog_request(dist, node, Method::GET, &format!("/iceberg/v1/buckets/{bucket}"), None, StatusCode::OK).await?;
        assert_eq!(response["enabled"], enabled, "node {node}: {response}");
        assert_eq!(response["table-bucket"], bucket);
        let expected = if enabled { StatusCode::OK } else { StatusCode::BAD_REQUEST };
        catalog_request(dist, node, Method::GET, &format!("/iceberg/v1/{bucket}/namespaces"), None, expected).await?;
        assert_eq!(
            get_object_bytes(&dist.client(node)?, bucket, "ordinary.txt").await?,
            b"ordinary bucket object",
            "node {node}: toggling table support changed an ordinary object"
        );
    }
    Ok(())
}

async fn assert_table_state(dist: &DistCluster, table_path: &str, created: &Value, pointer: &Value) -> TestResult {
    for node in 0..dist.cluster.nodes.len() {
        let loaded = catalog_request(dist, node, Method::GET, table_path, None, StatusCode::OK).await?;
        assert_eq!(loaded["metadata-location"], created["metadata-location"], "node {node}");
        assert_eq!(loaded["metadata"], created["metadata"], "node {node}");
        let actual_pointer =
            catalog_request(dist, node, Method::GET, &format!("{table_path}/metadata-location"), None, StatusCode::OK).await?;
        assert_eq!(&actual_pointer, pointer, "node {node}: catalog pointer changed");
    }
    Ok(())
}

#[tokio::test]
async fn table_bucket_toggles_reload_cached_metadata_on_every_peer() -> TestResult {
    init_logging();
    let dist = DistCluster::start_with_env(DistLayout::FourByFour, &[("RUSTFS_TABLE_CATALOG_BACKING", "object")]).await?;
    let bucket = unique_bucket("table-peer-reload");
    dist.create_bucket(&bucket).await?;
    put_object(&dist.client(0)?, &bucket, "ordinary.txt", b"ordinary bucket object".to_vec()).await?;
    let bucket_path = format!("/iceberg/v1/buckets/{bucket}");

    // Warm each node's false cache before changing the marker. Do not poll:
    // successful toggles must notify peers before their response is returned.
    assert_bucket_state(&dist, &bucket, false).await?;
    for (node, method, enabled) in [(0, Method::PUT, true), (1, Method::DELETE, false), (2, Method::PUT, true)] {
        let response = catalog_request(&dist, node, method, &bucket_path, None, StatusCode::OK).await?;
        assert_eq!(response["enabled"], enabled);
        assert_bucket_state(&dist, &bucket, enabled).await?;
    }

    let namespaces = format!("/iceberg/v1/{bucket}/namespaces");
    catalog_request(&dist, 3, Method::POST, &namespaces, Some(json!({"namespace": ["lab"]})), StatusCode::OK).await?;
    let tables = format!("{namespaces}/lab/tables");
    let created = catalog_request(
        &dist,
        0,
        Method::POST,
        &tables,
        Some(json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]
            },
            "stage-create": false
        })),
        StatusCode::OK,
    )
    .await?;
    let uuid = created["metadata"]["table-uuid"]
        .as_str()
        .ok_or("create response omitted table UUID")?;
    assert!(!uuid::Uuid::parse_str(uuid)?.is_nil());
    assert!(
        created["metadata-location"]
            .as_str()
            .is_some_and(|location| location.starts_with(&format!("s3://{bucket}/")))
    );
    let table_path = format!("{tables}/events");
    let pointer =
        catalog_request(&dist, 3, Method::GET, &format!("{table_path}/metadata-location"), None, StatusCode::OK).await?;
    assert_eq!(pointer["metadata-location"], created["metadata-location"]);
    assert!(pointer["version-token"].as_str().is_some_and(|token| !token.is_empty()));
    assert!(pointer["generation"].as_u64().is_some());
    assert_eq!(pointer["warehouse-location"], created["metadata"]["location"]);
    assert_table_state(&dist, &table_path, &created, &pointer).await?;

    // A rejected toggle must preserve both peer protections and catalog state.
    catalog_request(&dist, 1, Method::DELETE, &bucket_path, None, StatusCode::CONFLICT).await?;
    assert_bucket_state(&dist, &bucket, true).await?;
    assert_table_state(&dist, &table_path, &created, &pointer).await?;

    catalog_request(&dist, 2, Method::DELETE, &table_path, None, StatusCode::NO_CONTENT).await?;
    catalog_request(&dist, 3, Method::DELETE, &format!("{namespaces}/lab"), None, StatusCode::NO_CONTENT).await?;
    // The cluster fixture owns all remaining catalog objects and data roots.
    Ok(())
}
