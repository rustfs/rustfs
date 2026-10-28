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

use crate::common::{RustFSTestClusterEnvironment, init_logging};
use crate::storage_api::multipart_fence_acceptance::RUSTFS_META_MULTIPART_BUCKET;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::time::{Duration, Instant, sleep};
use tracing::info;
use uuid::Uuid;

const CLEANUP_BUCKET: &str = "stale-multipart-cleanup-cluster";

async fn list_parts_reports_missing_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let result = client.list_parts().bucket(bucket).key(key).upload_id(upload_id).send().await;
    match result {
        Ok(_) => Ok(false),
        Err(SdkError::ServiceError(err)) => {
            let code = err.err().meta().code().unwrap_or("");
            if code == "NoSuchUpload" {
                Ok(true)
            } else {
                Err(format!("unexpected list_parts service error: code={code}, err={err:?}").into())
            }
        }
        Err(err) => Err(format!("unexpected list_parts error: {err:?}").into()),
    }
}

async fn multipart_listing_reports_missing_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let result = client.list_multipart_uploads().bucket(bucket).prefix(key).send().await?;

    Ok(!result
        .uploads()
        .iter()
        .any(|upload| upload.key() == Some(key) && upload.upload_id() == Some(upload_id)))
}

async fn wait_for_cleanup_on_all_nodes(
    clients: &[aws_sdk_s3::Client],
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for attempt in 0..30 {
        let mut all_cleaned = true;
        for (idx, client) in clients.iter().enumerate() {
            let list_parts_missing = list_parts_reports_missing_upload(client, bucket, key, upload_id).await?;
            let listing_missing = multipart_listing_reports_missing_upload(client, bucket, key, upload_id).await?;
            if !(list_parts_missing && listing_missing) {
                info!("stale multipart still visible on node {} at attempt {}", idx, attempt + 1);
                all_cleaned = false;
                break;
            }
        }

        if all_cleaned {
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;
    }

    Err("stale multipart upload was not cleaned up on all nodes within timeout".into())
}

type PhysicalTree = BTreeMap<PathBuf, Option<Vec<u8>>>;

fn physical_tree(root: &Path) -> Result<PhysicalTree, Box<dyn std::error::Error + Send + Sync>> {
    let mut result = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(root)?.to_path_buf();
            let kind = entry.file_type()?;
            if kind.is_dir() {
                result.insert(relative, None);
                pending.push(path);
            } else if kind.is_file() {
                result.insert(relative, Some(std::fs::read(path)?));
            } else {
                return Err(format!("unexpected staging entry type: {relative:?}").into());
            }
        }
    }
    Ok(result)
}

async fn assert_low_quorum_residuals_are_diagnosed_and_preserved(
    cluster: &RustFSTestClusterEnvironment,
    clients: &[aws_sdk_s3::Client],
    log_paths: &[PathBuf],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut residuals = Vec::new();
    for copies in [1_usize, 2] {
        let key = format!("multipart/partial-{copies}-{}", Uuid::new_v4().simple());
        let upload = clients[0]
            .create_multipart_upload()
            .bucket(CLEANUP_BUCKET)
            .key(&key)
            .send()
            .await?;
        let upload_id = upload.upload_id().ok_or("create response missing upload ID")?;
        clients[0]
            .upload_part()
            .bucket(CLEANUP_BUCKET)
            .key(&key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0x41; 4096]))
            .send()
            .await?;
        let decoded = String::from_utf8(base64_simd::URL_SAFE_NO_PAD.decode_to_vec(upload_id.as_bytes())?)?;
        let (_, upload_dir) = decoded.split_once('.').ok_or("upload ID lacks deployment delimiter")?;
        let digest = Sha256::digest(format!("{CLEANUP_BUCKET}/{key}").as_bytes());
        let hash = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
        let relative = format!("{hash}/{upload_dir}");
        let roots = cluster
            .nodes
            .iter()
            .map(|node| {
                Path::new(&node.data_dirs[0])
                    .join(RUSTFS_META_MULTIPART_BUCKET)
                    .join(&relative)
            })
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 4);
        let mut preserved = Vec::new();
        for root in &roots {
            assert!(
                root.join("xl.meta").is_file(),
                "all four original metadata files must exist before injection"
            );
            preserved.push(physical_tree(root)?);
        }
        for root in roots.iter().skip(copies) {
            std::fs::remove_dir_all(root)?;
        }
        residuals.push((relative, roots, preserved, copies));
    }

    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let logs = log_paths
            .iter()
            .filter_map(|path| std::fs::read_to_string(path).ok())
            .collect::<Vec<_>>();
        let diagnosed = residuals.iter().all(|(path, _, _, _)| {
            logs.iter().any(|log| {
                log.lines()
                    .any(|line| line.contains(path) && line.contains("multipart_cleanup_lock_or_recheck_failed"))
            })
        });
        if diagnosed {
            break;
        }
        if Instant::now() >= deadline {
            return Err("stale low-quorum residuals were not diagnosed before the deadline".into());
        }
        sleep(Duration::from_millis(200)).await;
    }
    for (path, roots, preserved, copies) in &residuals {
        for slot in 0..*copies {
            assert_eq!(
                physical_tree(&roots[slot])?,
                preserved[slot],
                "retained source evidence changed: {path}, slot {slot}"
            );
        }
        for root in roots.iter().skip(*copies) {
            assert!(!root.exists(), "cleanup must not recreate missing source roots: {path}");
        }
    }
    for path in log_paths {
        let log = std::fs::read_to_string(path)?;
        assert!(
            !log.contains("op=\"upload_part\"") && !log.contains("\"op\":\"upload_part\""),
            "background cleanup must not be logged as a client UploadPart"
        );
    }
    eprintln!("Stale MPU low-quorum: EC:2 one/two-copy roots diagnosed; full surviving file inventories preserved");
    Ok(())
}

#[tokio::test]
async fn test_stale_multipart_cleanup_removes_incomplete_upload_across_cluster()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_API_STALE_UPLOADS_EXPIRY", "5s");
    cluster.set_env("RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL", "1s");
    cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2");
    cluster.set_env(
        "RUST_LOG",
        concat!("warn,rustfs_ecstore", "::bucket::lifecycle::bucket_lifecycle_ops=debug"),
    );
    let log_dir = std::env::var_os("RUSTFS_E2E_LOG_DIR")
        .map(PathBuf::from)
        .map(|root| root.join(format!("stale-multipart-{}", Uuid::new_v4().simple())))
        .unwrap_or_else(|| PathBuf::from(&cluster.temp_dir));
    std::fs::create_dir_all(&log_dir)?;
    let log_paths = (0..cluster.nodes.len())
        .map(|index| log_dir.join(format!("stale-node-{index}.log")))
        .collect::<Vec<_>>();
    for (index, path) in log_paths.iter().enumerate() {
        cluster.set_node_capture_log_path(index, path.to_string_lossy().into_owned())?;
    }
    cluster.start().await?;
    cluster.create_test_bucket(CLEANUP_BUCKET).await?;

    let clients = cluster.create_all_clients()?;
    let key = format!("multipart/stale-{}.txt", Uuid::new_v4().simple());

    let create_output = clients[0]
        .create_multipart_upload()
        .bucket(CLEANUP_BUCKET)
        .key(&key)
        .send()
        .await?;
    let upload_id = create_output
        .upload_id()
        .ok_or("create_multipart_upload response missing upload_id")?
        .to_string();

    clients[1]
        .upload_part()
        .bucket(CLEANUP_BUCKET)
        .key(&key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"stale multipart part"))
        .send()
        .await?;

    let parts_before_cleanup = clients[2]
        .list_parts()
        .bucket(CLEANUP_BUCKET)
        .key(&key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(
        parts_before_cleanup.parts().len(),
        1,
        "multipart upload should be visible before background cleanup"
    );
    assert!(
        !multipart_listing_reports_missing_upload(&clients[2], CLEANUP_BUCKET, &key, &upload_id).await?,
        "multipart upload listing should contain the upload before background cleanup"
    );

    wait_for_cleanup_on_all_nodes(&clients, CLEANUP_BUCKET, &key, &upload_id).await?;
    assert_low_quorum_residuals_are_diagnosed_and_preserved(&cluster, &clients, &log_paths).await?;

    Ok(())
}
