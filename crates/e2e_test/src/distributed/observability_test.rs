// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::harness::{
    DistCluster, DistLayout, TestResult, cluster_admin, cluster_admin_ok, put_object, unique_bucket, wait_for_ready,
};
use crate::common::{init_logging, local_http_client};
use http::Method;

#[tokio::test]
async fn four_node_four_drive_health_admin_info_and_audit_list() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    wait_for_ready(&dist.cluster).await?;

    let http = local_http_client();
    for node in &dist.cluster.nodes {
        let ready = http.get(format!("{}/health/ready", node.url)).send().await?;
        assert!(ready.status().is_success(), "node {} not ready: {}", node.address, ready.status());
        let live = http.get(format!("{}/health/live", node.url)).send().await;
        if let Ok(response) = live {
            assert!(
                response.status().is_success() || response.status().as_u16() == 404,
                "unexpected live probe on {}: {}",
                node.address,
                response.status()
            );
        }
    }

    let info = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/info", None).await?;
    assert!(!info.is_empty(), "admin info was empty");
    let storage = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    assert!(
        storage.contains("disks") || storage.contains("backend") || storage.contains("info"),
        "storageinfo missing expected fields: {storage}"
    );

    let audit = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/audit/target/list", None).await?;
    let trimmed = audit.trim();
    if !trimmed.is_empty() && trimmed != "null" && !trimmed.starts_with('[') && !trimmed.starts_with('{') {
        return Err(format!("audit target list was not machine-readable: {audit}").into());
    }

    // Logs / capabilities surfaces: 404 is acceptable (route not enabled);
    // 5xx is not. A 2xx body must be non-empty.
    for path in [
        "/rustfs/admin/v3/log/search",
        "/rustfs/admin/v4/runtime/capabilities",
        "/minio/v2/metrics/cluster",
    ] {
        let (status, body) = cluster_admin(&dist.cluster, Method::GET, path, None).await?;
        assert!(
            status.is_success() || status.is_client_error(),
            "observability path {path} returned {status}: {body}"
        );
        if status.is_success() {
            assert!(!body.trim().is_empty(), "empty body from {path}");
        }
    }

    let bucket = unique_bucket("obs");
    dist.create_bucket(&bucket).await?;
    put_object(&dist.client(0)?, &bucket, "probe.log", b"observability".to_vec()).await?;

    let trace = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/info", None).await?;
    assert!(!trace.is_empty());
    Ok(())
}
