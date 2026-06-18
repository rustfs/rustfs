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

use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::common::{RustFSTestEnvironment, init_logging};

const TEST_BUCKET: &str = "overwrite-cleanup-regression";
const TEST_OBJECT: &str = "large-object.bin";
const PAYLOAD_SIZE: usize = 512 * 1024;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unversioned_overwrite_removes_previous_physical_data_dir() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let client = env.create_s3_client();
    client.create_bucket().bucket(TEST_BUCKET).send().await?;

    let first_payload = vec![b'a'; PAYLOAD_SIZE];
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(TEST_OBJECT)
        .body(ByteStream::from(first_payload))
        .send()
        .await?;

    let object_dir = Path::new(&env.temp_dir).join(TEST_BUCKET).join(TEST_OBJECT);
    let first_data_dirs = physical_data_dirs(&object_dir)?;
    assert_eq!(
        first_data_dirs.len(),
        1,
        "first non-inline put should create exactly one physical data directory: {first_data_dirs:?}"
    );

    let second_payload = vec![b'z'; PAYLOAD_SIZE];
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(TEST_OBJECT)
        .body(ByteStream::from(second_payload.clone()))
        .send()
        .await?;

    let second_data_dirs = physical_data_dirs(&object_dir)?;
    assert_eq!(
        second_data_dirs.len(),
        1,
        "unversioned overwrite should clean the previous physical data directory: {second_data_dirs:?}"
    );
    assert_ne!(
        first_data_dirs[0], second_data_dirs[0],
        "overwrite should replace the old physical data directory with the new one"
    );

    let current = client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(TEST_OBJECT)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(
        current.as_ref(),
        second_payload.as_slice(),
        "object data should remain readable after old data directory cleanup"
    );

    env.stop_server();
    Ok(())
}

fn physical_data_dirs(object_dir: &Path) -> Result<Vec<Uuid>, Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dirs = Vec::new();
    for entry in std::fs::read_dir(object_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let path: PathBuf = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Ok(uuid) = Uuid::parse_str(name) {
            data_dirs.push(uuid);
        }
    }
    data_dirs.sort_unstable();
    Ok(data_dirs)
}
