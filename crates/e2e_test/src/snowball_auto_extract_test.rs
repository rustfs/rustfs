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

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use std::error::Error;
    use std::path::PathBuf;
    use std::process::Command;
    use tokio::fs;

    async fn build_test_archive(
        env: &RustFSTestEnvironment,
        archive_name: &str,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let source_dir = PathBuf::from(&env.temp_dir).join(format!("{archive_name}-src"));
        let archive_path = PathBuf::from(&env.temp_dir).join(format!("{archive_name}.tar"));

        fs::create_dir_all(source_dir.join("dir")).await?;
        fs::create_dir_all(source_dir.join("empty-dir")).await?;
        fs::write(source_dir.join("dir/file.txt"), b"nested payload\n").await?;
        fs::write(source_dir.join("root.txt"), b"root payload\n").await?;

        let output = Command::new("tar")
            .args([
                "-cf",
                archive_path.to_str().expect("archive path should be valid UTF-8"),
                "-C",
                source_dir.to_str().expect("source path should be valid UTF-8"),
                "dir",
                "empty-dir",
                "root.txt",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!("tar command failed: stderr='{}'", String::from_utf8_lossy(&output.stderr)).into());
        }

        Ok(fs::read(archive_path).await?)
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_supports_minio_prefix_and_directory_markers() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-prefix-test";
        let archive = build_test_archive(&env, "prefix").await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "/tenant-a/")
            .body(ByteStream::from(archive))
            .send()
            .await?;

        let root = client.get_object().bucket(bucket).key("tenant-a/root.txt").send().await?;
        assert_eq!(root.body.collect().await?.into_bytes().as_ref(), b"root payload\n");

        let nested = client.get_object().bucket(bucket).key("tenant-a/dir/file.txt").send().await?;
        assert_eq!(nested.body.collect().await?.into_bytes().as_ref(), b"nested payload\n");

        let dir_marker = client.head_object().bucket(bucket).key("tenant-a/empty-dir/").send().await?;
        assert_eq!(dir_marker.content_length(), Some(0));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_ignores_directories_when_requested() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-ignore-dirs-default";
        let archive = build_test_archive(&env, "ignore-dirs").await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "tenant-b")
            .metadata("Minio-Snowball-Ignore-Dirs", "true")
            .body(ByteStream::from(archive))
            .send()
            .await?;

        let err = client
            .head_object()
            .bucket(bucket)
            .key("tenant-b/empty-dir/")
            .send()
            .await
            .expect_err("directory marker should be skipped when ignore-dirs=true");
        let service_err = err.into_service_error();
        assert_eq!(service_err.code(), Some("NotFound"));

        env.stop_server();
        Ok(())
    }
}
