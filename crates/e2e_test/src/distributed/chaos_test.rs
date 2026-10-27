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
    DistCluster, DistLayout, TestResult, assert_object_bytes, cluster_admin_ok, get_object_bytes, payload_for, put_object,
    retrying_get_equals, retrying_put, s3_probe_client, s3_probe_error_code, unique_bucket, wait_for_ready, wait_until,
};
use crate::chaos::{census_object_version_on_disk, signed_admin_post};
use crate::common::{build_test_s3_config, init_logging};
use crate::fault_proxy::FaultMode;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::{get_object::GetObjectError, put_object::PutObjectError};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Barrier, mpsc};
use tokio::time::{Instant, timeout};

#[tokio::test]
async fn kill_and_restart_node_preserves_objects() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("killnode");
    dist.create_bucket(&bucket).await?;
    let body = vec![0x11u8; 128 * 1024];
    put_object(&dist.client(0)?, &bucket, "keep.bin", body.clone()).await?;

    dist.cluster.stop_node(3)?;
    retrying_get_equals(&dist.client(0)?, &bucket, "keep.bin", &body, Duration::from_secs(20)).await?;

    dist.cluster.start_node(3).await?;
    wait_for_ready(&dist.cluster).await?;
    assert_object_bytes(&dist.client(3)?, &bucket, "keep.bin", &body).await?;
    Ok(())
}

#[tokio::test]
async fn ec4_partial_node_return_restores_read_write_quorum() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start_with_env(
        DistLayout::FourByFour,
        &[
            ("RUSTFS_ERASURE_SET_DRIVE_COUNT", "16"),
            ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:4"),
        ],
    )
    .await?;
    let storage = cluster_admin_ok(&dist.cluster, http::Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    let storage: serde_json::Value = serde_json::from_str(&storage)?;
    let storage: rustfs_madmin::StorageInfo = serde_json::from_value(storage["info"].clone())?;
    assert_eq!(storage.disks.len(), 16);
    assert_eq!(storage.backend.total_sets, vec![1]);
    assert_eq!(storage.backend.drives_per_set, vec![16]);
    assert_eq!(storage.backend.standard_sc_parity, Some(4));

    let bucket = unique_bucket("partial-ec4");
    dist.create_bucket(&bucket).await?;
    let client = s3_probe_client(&dist.client(0)?);
    let baseline = payload_for("partial-recovery/baseline", 8 * 1024 * 1024);
    put_object(&client, &bucket, "baseline.bin", baseline.clone()).await?;
    wait_until(
        Duration::from_secs(30),
        || async {
            for node in &dist.cluster.nodes {
                for directory in &node.data_dirs {
                    let census = census_object_version_on_disk(std::path::Path::new(directory), &bucket, "baseline.bin", None)?;
                    if !census.is_complete() {
                        return Ok(false);
                    }
                    assert_eq!(census.data_blocks, Some(12), "the object must require twelve data shards");
                    assert_eq!(census.parity_blocks, Some(4), "the object must have four parity shards");
                    assert!(!census.expected_part_numbers.is_empty(), "the baseline must use physical shards");
                }
            }
            Ok(true)
        },
        "the baseline is complete on all sixteen EC:4 drives",
    )
    .await?;

    dist.cluster.stop_node(2)?;
    dist.cluster.stop_node(3)?;
    let read_error = timeout(Duration::from_secs(60), get_object_bytes(&client, &bucket, "baseline.bin"))
        .await?
        .expect_err("eight unavailable drives must prevent the baseline GET");
    assert!(
        matches!(
            s3_probe_error_code(read_error.as_ref()),
            Some("SlowDown" | "SlowDownRead" | "ServiceUnavailable")
        ),
        "quorum loss must produce an availability error: {read_error:?}"
    );
    assert_eq!(
        read_error
            .downcast_ref::<SdkError<GetObjectError>>()
            .and_then(SdkError::raw_response)
            .map(|response| response.status().as_u16()),
        Some(503),
        "read quorum loss must remain a service availability failure"
    );
    let write_error = timeout(Duration::from_secs(60), put_object(&client, &bucket, "unavailable.bin", baseline.clone()))
        .await?
        .expect_err("eight unavailable drives must prevent a successful PUT");
    assert!(
        matches!(s3_probe_error_code(write_error.as_ref()), Some("SlowDown" | "ServiceUnavailable")),
        "quorum loss must produce an availability error: {write_error:?}"
    );
    assert_eq!(
        write_error
            .downcast_ref::<SdkError<PutObjectError>>()
            .and_then(SdkError::raw_response)
            .map(|response| response.status().as_u16()),
        Some(503),
        "write quorum loss must remain a service availability failure"
    );

    // Recover only four drives. The remaining node must stay stopped while
    // every live coordinator serves old data and acknowledges new writes.
    dist.cluster.start_node_process(2).await?;
    let recovery_deadline = Instant::now() + Duration::from_secs(90);
    let mut recovered_objects = Vec::new();
    for node in 0..3 {
        assert!(dist.cluster.nodes[3].process.is_none(), "the fourth node must remain unavailable");
        let client = s3_probe_client(&dist.client(node)?);
        retrying_get_equals(
            &client,
            &bucket,
            "baseline.bin",
            &baseline,
            recovery_deadline.saturating_duration_since(Instant::now()),
        )
        .await?;
        let key = format!("partial-{node}.bin");
        let body = payload_for(&key, 1024 * 1024);
        retrying_put(
            &client,
            &bucket,
            &key,
            body.clone(),
            recovery_deadline.saturating_duration_since(Instant::now()),
        )
        .await?;
        recovered_objects.push((key, body));
    }
    for node in 0..3 {
        assert!(dist.cluster.nodes[3].process.is_none(), "reads must complete before all drives return");
        let client = s3_probe_client(&dist.client(node)?);
        assert_object_bytes(&client, &bucket, "baseline.bin", &baseline).await?;
        for (key, body) in &recovered_objects {
            assert_object_bytes(&client, &bucket, key, body).await?;
        }
    }

    dist.cluster.start_node(3).await?;
    wait_for_ready(&dist.cluster).await?;
    let client = s3_probe_client(&dist.client(3)?);
    assert_object_bytes(&client, &bucket, "baseline.bin", &baseline).await?;
    for (key, body) in &recovered_objects {
        assert_object_bytes(&client, &bucket, key, body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn full_cluster_restart_preserves_objects() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("pwr");
    dist.create_bucket(&bucket).await?;
    let body = vec![0x44u8; 64 * 1024];
    put_object(&dist.client(1)?, &bucket, "survive.bin", body.clone()).await?;

    dist.cluster.stop();
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;
    for node_idx in 0..dist.cluster.nodes.len() {
        assert_object_bytes(&dist.client(node_idx)?, &bucket, "survive.bin", &body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn fresh_drive_replacement_is_physically_healed_without_data_change() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start_with_env(DistLayout::FourByFour, &[("RUSTFS_HEAL_ENABLED", "true")]).await?;
    let bucket = unique_bucket("baddrive");
    dist.create_bucket(&bucket).await?;
    let body = payload_for("fresh-drive/durable.bin", 8 * 1024 * 1024);
    put_object(&dist.client(1)?, &bucket, "durable.bin", body.clone()).await?;

    let replaced_drive = PathBuf::from(&dist.cluster.nodes[0].data_dirs[0]);
    let baseline = census_object_version_on_disk(&replaced_drive, &bucket, "durable.bin", None)?;
    assert!(
        baseline.is_complete(),
        "replacement target did not hold a complete baseline shard: {baseline:?}"
    );
    assert!(
        !baseline.expected_part_numbers.is_empty(),
        "replacement witness must use physical part shards: {baseline:?}"
    );

    dist.cluster.stop_node(0)?;
    let format_path = replaced_drive.join(".rustfs.sys/format.json");
    let format = std::fs::read(&format_path)?;
    let retired_drive = PathBuf::from(format!("{}.retired", replaced_drive.display()));
    std::fs::rename(&replaced_drive, &retired_drive)?;
    std::fs::create_dir_all(format_path.parent().ok_or("replacement format path omitted parent")?)?;
    std::fs::write(&format_path, format)?;
    let empty = census_object_version_on_disk(&replaced_drive, &bucket, "durable.bin", None)?;
    assert!(!empty.has_xl_meta, "fresh replacement unexpectedly retained object metadata: {empty:?}");

    dist.cluster.start_node(0).await?;
    wait_for_ready(&dist.cluster).await?;
    let heal_body =
        r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
    let heal_url = format!("{}/rustfs/admin/v3/heal/{bucket}?forceStart=true", dist.cluster.nodes[1].url);
    signed_admin_post(&heal_url, Some(heal_body), &dist.cluster.access_key, &dist.cluster.secret_key).await?;
    wait_until(
        Duration::from_secs(90),
        || async {
            let healed = census_object_version_on_disk(&replaced_drive, &bucket, "durable.bin", None)?;
            Ok(healed.matches_manifest(&baseline))
        },
        "fresh replacement contains the original complete shard manifest",
    )
    .await?;

    for node_idx in 0..dist.cluster.nodes.len() {
        assert_object_bytes(&dist.client(node_idx)?, &bucket, "durable.bin", &body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_gets_survive_peer_node_kill() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("getkill");
    dist.create_bucket(&bucket).await?;
    let body = payload_for("inflight/steady.bin", 8 * 1024 * 1024);
    put_object(&dist.client(0)?, &bucket, "steady.bin", body.clone()).await?;

    let live: Vec<_> = (0..3).map(|idx| dist.client(idx)).collect::<Result<Vec<_>, _>>()?;
    let worker_count = 12;
    let release = Arc::new(Barrier::new(worker_count + 1));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let mut handles = Vec::new();
    for idx in 0..worker_count {
        let client = live[idx % live.len()].clone();
        let bucket = bucket.clone();
        let body = body.clone();
        let release = release.clone();
        let started_tx = started_tx.clone();
        handles.push(tokio::spawn(async move {
            let response = client.get_object().bucket(&bucket).key("steady.bin").send().await?;
            if response.content_length() != Some(body.len() as i64) {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    format!("worker {idx} received a wrong content length").into(),
                );
            }
            started_tx.send(idx)?;
            release.wait().await;
            let actual = response.body.collect().await?.into_bytes();
            if actual.as_ref() != body.as_slice() {
                return Err(format!("worker {idx} received corrupted bytes after peer kill").into());
            }
            Ok(())
        }));
    }
    drop(started_tx);
    for _ in 0..worker_count {
        timeout(Duration::from_secs(30), started_rx.recv())
            .await?
            .ok_or("a streaming GET exited before reaching the kill barrier")?;
    }

    dist.cluster.stop_node(3)?;
    release.wait().await;
    for handle in handles {
        handle.await??;
    }

    dist.cluster.start_node(3).await?;
    wait_for_ready(&dist.cluster).await?;
    assert_object_bytes(&dist.client(3)?, &bucket, "steady.bin", &body).await?;
    Ok(())
}

#[tokio::test]
async fn blackholed_node_client_network_preserves_cluster_availability_and_recovers() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let proxy = crate::fault_proxy::FaultProxy::start(dist.cluster.nodes[3].address.parse()?).await?;
    let proxied_url = format!("http://{}", proxy.local_addr());
    let proxied_client = Client::from_conf(build_test_s3_config(
        &proxied_url,
        &dist.cluster.access_key,
        &dist.cluster.secret_key,
        None,
        "distributed-network-chaos",
    ));

    let result: TestResult = async {
        let bucket = unique_bucket("netfault");
        dist.create_bucket(&bucket).await?;
        let baseline = payload_for("network/baseline.bin", 1024 * 1024);
        put_object(&dist.client(0)?, &bucket, "baseline.bin", baseline.clone()).await?;
        assert_object_bytes(&proxied_client, &bucket, "baseline.bin", &baseline).await?;

        proxy.set_mode(FaultMode::Blackhole);
        assert_eq!(proxy.mode(), FaultMode::Blackhole);
        if let Ok(Ok(_)) = timeout(
            Duration::from_secs(5),
            proxied_client.get_object().bucket(&bucket).key("baseline.bin").send(),
        )
        .await
        {
            return Err("blackholed node endpoint unexpectedly completed a GET".into());
        }

        let during = payload_for("network/during.bin", 1024 * 1024);
        timeout(Duration::from_secs(30), async {
            put_object(&dist.client(1)?, &bucket, "during-blackhole.bin", during.clone()).await?;
            assert_object_bytes(&dist.client(2)?, &bucket, "baseline.bin", &baseline).await?;
            assert_object_bytes(&dist.client(0)?, &bucket, "during-blackhole.bin", &during).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;

        proxy.set_mode(FaultMode::Pass);
        retrying_get_equals(&proxied_client, &bucket, "during-blackhole.bin", &during, Duration::from_secs(30)).await?;
        Ok(())
    }
    .await;

    proxy.set_mode(FaultMode::Pass);
    proxy.shutdown().await;
    result
}
