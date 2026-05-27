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

use crate::common::RustFSTestClusterEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use bytes::Bytes;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::Barrier;
use tracing::{info, warn};

const BUCKET: &str = "namespace-lock-quorum-bucket";
const KEY: &str = "thumb/79/concurrent-overwrite.jpg";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn put_object(client: Client, payload: Vec<u8>, writer_id: usize) -> Result<(), String> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(Bytes::from(payload).into())
        .send()
        .await
        .map(|_| ())
        .map_err(|err| format_s3_error(err, writer_id))
}

fn format_s3_error(err: SdkError<aws_sdk_s3::operation::put_object::PutObjectError>, writer_id: usize) -> String {
    match err {
        SdkError::ServiceError(service_err) => {
            let err = service_err.err();
            let code = err.meta().code().unwrap_or("<unknown>");
            let message = err.meta().message().unwrap_or("<empty>");
            format!("writer {writer_id} returned service error {code}: {message}")
        }
        other => format!("writer {writer_id} returned SDK error: {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn test_concurrent_cluster_overwrites_do_not_fail_namespace_lock_quorum() -> TestResult {
    crate::common::init_logging();
    info!("Starting namespace lock quorum regression test with auto cluster");

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    // Keep the regression focused on false quorum-loss errors, not ordinary lock
    // wait exhaustion under a heavily contended same-key overwrite workload.
    cluster.set_env("RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT", "20");
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let clients = cluster.create_all_clients()?;
    let first_payload = b"initial object contents".to_vec();
    put_object(clients[0].clone(), first_payload, 0).await?;

    let writer_count = clients.len() * 2;
    let barrier = Arc::new(Barrier::new(writer_count));
    let mut handles = Vec::with_capacity(writer_count);

    for writer_id in 0..writer_count {
        let client = clients[writer_id % clients.len()].clone();
        let barrier = barrier.clone();
        let payload = format!("replacement payload from writer {writer_id:02}").into_bytes();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            put_object(client, payload, writer_id).await
        }));
    }

    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => failures.push(err),
            Err(err) => failures.push(format!("writer task join failed: {err}")),
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            warn!("concurrent overwrite failure: {}", failure);
        }
    }

    assert!(
        failures.is_empty(),
        "concurrent overwrites must not surface namespace lock quorum failures: {failures:#?}"
    );

    let body = clients[0]
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    let body = std::str::from_utf8(&body)?;
    assert!(
        body.starts_with("replacement payload from writer "),
        "final object body should be one of the successful overwrite payloads, got {body:?}"
    );

    clients[0].delete_object().bucket(BUCKET).key(KEY).send().await.ok();
    Ok(())
}
