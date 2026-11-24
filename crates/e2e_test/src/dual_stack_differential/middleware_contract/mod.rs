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

//! Wire contracts for the S3 middleware shared by the candidate and released server.
use super::{BoxError, RustFSTestEnvironment, SideA, rustfs_binary_path};

mod cors;
mod object;

async fn check(side: SideA) -> Result<(), BoxError> {
    let binary = match side {
        SideA::SameBinary => rustfs_binary_path(),
        SideA::Reference(path) => path,
    };
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_from_binary_without_cleanup(
        &binary,
        vec![],
        &[
            ("RUSTFS_CHECK_UPDATE", "false"),
            ("RUSTFS_SERVER_DOMAINS", "s3.test"),
            ("RUSTFS_COMPRESS_ENABLE", "true"),
            ("RUSTFS_COMPRESS_MIN_SIZE", "1000"),
            ("RUSTFS_COMPRESS_MIME_TYPES", "text/*"),
            ("RUSTFS_COMPRESS_EXTENSIONS", ""),
        ],
    )
    .await?;
    let bucket = format!("middleware-{}", uuid::Uuid::new_v4().simple());
    env.create_s3_client().create_bucket().bucket(&bucket).send().await?;
    cors::check(&env, &bucket).await?;
    object::check(&env, &bucket).await
}

#[tokio::test]
async fn current_middleware_contract() -> Result<(), BoxError> {
    check(SideA::SameBinary).await
}

#[tokio::test]
async fn reference_middleware_contract() -> Result<(), BoxError> {
    check(SideA::from_env()?).await
}
