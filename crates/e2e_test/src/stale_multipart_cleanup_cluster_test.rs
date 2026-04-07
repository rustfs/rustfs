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
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use serial_test::serial;
use tokio::time::{Duration, sleep};
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

async fn complete_reports_missing_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let result = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().build())
        .send()
        .await;
    match result {
        Ok(_) => Ok(false),
        Err(SdkError::ServiceError(err)) => {
            let code = err.err().meta().code().unwrap_or("");
            if code == "NoSuchUpload" {
                Ok(true)
            } else {
                Err(format!("unexpected complete_multipart_upload service error: code={code}, err={err:?}").into())
            }
        }
        Err(err) => Err(format!("unexpected complete_multipart_upload error: {err:?}").into()),
    }
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
            let complete_missing = complete_reports_missing_upload(client, bucket, key, upload_id).await?;
            if !(list_parts_missing && complete_missing) {
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

#[tokio::test]
#[serial]
async fn test_stale_multipart_cleanup_removes_incomplete_upload_across_cluster()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_API_STALE_UPLOADS_EXPIRY", "5s");
    cluster.set_env("RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL", "1s");
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

    wait_for_cleanup_on_all_nodes(&clients, CLEANUP_BUCKET, &key, &upload_id).await?;

    Ok(())
}
