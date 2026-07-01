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

use crate::common::{RustFSTestClusterEnvironment, init_logging, local_http_client};
use aws_sdk_s3::primitives::ByteStream;
use http::header::HOST;
use reqwest::StatusCode;
use rustfs_madmin::{ITEM_OFFLINE, InfoMessage, StorageInfo};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serde::Deserialize;
use serial_test::serial;
use std::error::Error;
use std::process::Command;
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

const BUCKET: &str = "issue-2525-admin-timeout";

#[derive(Deserialize)]
struct StorageInfoResponse {
    info: StorageInfo,
}

async fn signed_admin_get(
    url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .body(Body::empty())?;
    let signed = sign_v4(request, 0, access_key, secret_key, "", "us-east-1");

    let mut builder = local_http_client().get(url);
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }

    Ok(builder.send().await?)
}

async fn fetch_info(cluster: &RustFSTestClusterEnvironment) -> Result<InfoMessage, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/info", cluster.nodes[0].url);
    let response = signed_admin_get(&url, &cluster.access_key, &cluster.secret_key).await?;
    parse_json_response(response, "cluster info").await
}

async fn fetch_storage_info(cluster: &RustFSTestClusterEnvironment) -> Result<StorageInfo, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/storageinfo", cluster.nodes[0].url);
    let response = signed_admin_get(&url, &cluster.access_key, &cluster.secret_key).await?;
    let wrapper: StorageInfoResponse = parse_json_response(response, "storage info").await?;
    Ok(wrapper.info)
}

async fn parse_json_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    label: &str,
) -> Result<T, Box<dyn Error + Send + Sync>> {
    let status = response.status();
    let body = response.bytes().await?;
    if status != StatusCode::OK {
        return Err(format!("{label} request failed: {status} {}", String::from_utf8_lossy(body.as_ref())).into());
    }

    Ok(serde_json::from_slice(&body)?)
}

fn signal_process(pid: u32, signal: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let output = Command::new("kill").arg(format!("-{signal}")).arg(pid.to_string()).output()?;
    if output.status.success() {
        return Ok(());
    }

    Err(format!("kill -{signal} {pid} failed: {}", String::from_utf8_lossy(&output.stderr)).into())
}

fn offline_server_count(info: &InfoMessage) -> usize {
    info.servers
        .as_ref()
        .map(|servers| servers.iter().filter(|server| server.state == ITEM_OFFLINE).count())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_single_admin_timeout_does_not_immediately_mark_peer_offline() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let warm_info = fetch_info(&cluster).await?;
    assert_eq!(
        warm_info
            .backend
            .as_ref()
            .map(|backend| backend.offline_disks)
            .unwrap_or_default(),
        0,
        "cluster should start with zero offline disks"
    );

    let warm_storage = fetch_storage_info(&cluster).await?;
    assert_eq!(
        warm_storage.backend.offline_disks.sum(),
        0,
        "storage info should start with zero offline disks"
    );

    let suspended_pid = cluster.nodes[1]
        .process
        .as_ref()
        .ok_or("cluster node 1 process missing")?
        .id();
    signal_process(suspended_pid, "STOP")?;

    let resume = tokio::spawn(async move {
        sleep(Duration::from_secs(6)).await;
        signal_process(suspended_pid, "CONT")
    });

    let client = cluster.create_s3_client(0)?;
    let key = format!("issue-2525/{}", Uuid::new_v4().simple());
    let payload = b"admin timeout regression payload".to_vec();

    let during = timeout(Duration::from_secs(20), async {
        tokio::join!(fetch_info(&cluster), fetch_storage_info(&cluster), async {
            client
                .put_object()
                .bucket(BUCKET)
                .key(&key)
                .body(ByteStream::from(payload.clone()))
                .send()
                .await?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
    })
    .await;

    resume.await??;

    let (info_during, storage_during, put_result) =
        during.map_err(|_| "timed out while waiting for admin queries during suspended peer")?;
    let info_during = info_during?;
    let storage_during = storage_during?;
    put_result?;

    assert_eq!(
        info_during
            .backend
            .as_ref()
            .map(|backend| backend.offline_disks)
            .unwrap_or_default(),
        0,
        "single admin timeout must not synthesize offline disks in /info"
    );
    assert_eq!(
        offline_server_count(&info_during),
        0,
        "single admin timeout must not mark any server offline in /info"
    );
    assert_eq!(
        storage_during.backend.offline_disks.sum(),
        0,
        "single admin timeout must not synthesize offline disks in /storageinfo"
    );
    assert!(
        storage_during.disks.iter().all(|disk| disk.state != ITEM_OFFLINE),
        "single admin timeout must not synthesize offline disk entries in /storageinfo"
    );

    let recovered_info = fetch_info(&cluster).await?;
    assert_eq!(
        recovered_info
            .backend
            .as_ref()
            .map(|backend| backend.offline_disks)
            .unwrap_or_default(),
        0,
        "offline disk count should stay at zero after the peer resumes"
    );

    let body = client
        .get_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(body.as_ref(), b"admin timeout regression payload");

    Ok(())
}
