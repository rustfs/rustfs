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

//! Regression tests for distributed cluster startup and quorum.
//!
//! Covers the recurring pattern where multi-node clusters fail to start due to
//! lock quorum issues, DNS resolution delays, or erasure quorum deadlocks.
//! This has regressed 7+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5416: RustFS cannot cold-start with 2/3 quorum when Pod DNS missing
//! - rustfs#2945: Distributed mode fails on K8s: erasure quorum deadlock
//! - rustfs#2794: distributed deployment does not become ready
//! - rustfs#2601: fresh pod immediately enters FaultyDisk state
//! - rustfs#4040: Distributed startup can fail lock quorum before AppContext initializes
//! - rustfs#5655: fix(ecstore): bootstrap fresh four-node clusters reliably
//! - rustfs#4954: S3/health endpoint unavailability after multi-pool scale-up

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestClusterEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use std::error::Error;
    use tokio::time::{Duration, sleep};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    /// RT-10: Verify 4-node cluster starts successfully and all nodes are ready.
    ///
    /// Regression pattern: distributed startup fails with quorum deadlock or
    /// lock acquisition timeout (rustfs#2945, rustfs#5655).
    ///
    /// Steps:
    /// 1. Create a 4-node cluster
    /// 2. Start all nodes simultaneously
    /// 3. Verify all nodes report healthy
    /// 4. Verify S3 operations work through any node
    #[tokio::test]
    async fn test_four_node_cluster_startup_and_health() -> TestResult {
        init_logging();
        info!("RT-10: 4-node cluster startup and health");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start 4-node cluster");

        // Create a bucket and verify it's accessible from all nodes
        cluster
            .create_test_bucket("rt10-startup")
            .await
            .expect("create bucket on cluster");

        let clients = cluster.create_all_clients().expect("create per-node clients");

        // Verify S3 operations work from every node
        for (i, client) in clients.iter().enumerate() {
            client
                .put_object()
                .bucket("rt10-startup")
                .key(format!("from-node-{i}.txt"))
                .body(ByteStream::from_static(b"hello from node"))
                .send()
                .await
                .unwrap_or_else(|e| panic!("PUT from node {i} failed: {e}"));
        }

        // Verify all objects are visible from node 0
        let list = clients[0]
            .list_objects_v2()
            .bucket("rt10-startup")
            .send()
            .await
            .expect("list objects from node 0");

        assert_eq!(
            list.contents().len(),
            4,
            "RT-10 FAIL: expected 4 objects (one per node), found {}",
            list.contents().len()
        );

        info!("RT-10 PASS: 4-node cluster starts and serves S3 from all nodes");
        Ok(())
    }

    /// RT-10b: Verify cluster handles node restart gracefully.
    ///
    /// Regression pattern: after a node restart, it cannot rejoin the cluster
    /// or enters a faulty state (rustfs#2601).
    #[tokio::test]
    async fn test_cluster_survives_node_restart() -> TestResult {
        init_logging();
        info!("RT-10b: cluster survives node restart");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start cluster");

        cluster.create_test_bucket("rt10b-restart").await.expect("create bucket");

        // Write data
        let clients = cluster.create_all_clients()?;
        clients[0]
            .put_object()
            .bucket("rt10b-restart")
            .key("before-restart.txt")
            .body(ByteStream::from_static(b"persistent data"))
            .send()
            .await
            .expect("put object before restart");

        // Stop node 3
        cluster.stop_node(3).expect("stop node 3");
        sleep(Duration::from_secs(2)).await;

        // Verify cluster still works with 3/4 nodes (quorum)
        clients[0]
            .put_object()
            .bucket("rt10b-restart")
            .key("during-offline.txt")
            .body(ByteStream::from_static(b"written while node 3 down"))
            .send()
            .await
            .expect("PUT should succeed with 3/4 nodes");

        // Restart node 3
        cluster.start_node(3).await.expect("restart node 3");

        // Wait for node to rejoin
        sleep(Duration::from_secs(3)).await;

        // Verify the restarted node can serve reads
        let list = clients[3]
            .list_objects_v2()
            .bucket("rt10b-restart")
            .send()
            .await
            .expect("list from restarted node");

        assert!(
            list.contents().len() >= 2,
            "RT-10b FAIL: restarted node sees {} objects, expected >= 2",
            list.contents().len()
        );

        info!("RT-10b PASS: cluster survives and recovers from node restart");
        Ok(())
    }

    /// RT-10c: Verify bucket creation persists across all nodes.
    ///
    /// Regression pattern: bucket metadata is not replicated to all nodes,
    /// causing NoSuchBucket errors on some nodes (rustfs#3191).
    #[tokio::test]
    async fn test_bucket_visible_from_all_nodes() -> TestResult {
        init_logging();
        info!("RT-10c: bucket visible from all nodes");

        let mut cluster = RustFSTestClusterEnvironment::new(4).await.expect("create 4-node cluster");

        cluster.start().await.expect("start cluster");

        cluster
            .create_test_bucket("rt10c-bucket-visibility")
            .await
            .expect("create bucket");

        let clients = cluster.create_all_clients()?;

        // Verify the bucket is visible from every node
        for (i, client) in clients.iter().enumerate() {
            let resp = client
                .list_objects_v2()
                .bucket("rt10c-bucket-visibility")
                .send()
                .await
                .unwrap_or_else(|e| panic!("list from node {i} failed (NoSuchBucket?): {e}"));

            assert!(resp.contents().is_empty(), "RT-10c: fresh bucket should be empty on node {i}");
        }

        info!("RT-10c PASS: bucket visible from all 4 nodes");
        Ok(())
    }
}
