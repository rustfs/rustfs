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

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use http::header::HOST;
use reqwest::StatusCode;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize)]
struct PoolListItem {
    id: usize,
    cmdline: String,
    status: String,
}

async fn signed_admin_get(env: &RustFSTestEnvironment, path: &str) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}{path}", env.url);
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("admin URL missing authority")?.to_string();
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .body(Body::empty())?;
    let signed = sign_v4(request, 0, &env.access_key, &env.secret_key, "", "us-east-1");

    let mut request = local_http_client().get(&url);
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    Ok(request.send().await?)
}

#[tokio::test]
async fn single_drive_pools_list_succeeds_without_enabling_decommission_status() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = signed_admin_get(&env, "/rustfs/admin/v3/pools/list").await?;
    let status = response.status();
    let body = response.bytes().await?;

    assert_eq!(status, StatusCode::OK, "pools list failed: {}", String::from_utf8_lossy(&body));
    let pools: Vec<PoolListItem> = serde_json::from_slice(&body)?;
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].id, 0);
    assert_eq!(pools[0].cmdline, env.temp_dir);
    assert_eq!(pools[0].status, "active");

    let response = signed_admin_get(&env, "/rustfs/admin/v3/decommission/status").await?;
    let status = response.status();
    let body = response.text().await?;
    assert_eq!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "decommission status changed for a single pool: {body}"
    );
    assert!(body.contains("NotImplemented"), "unexpected decommission error body: {body}");

    Ok(())
}
