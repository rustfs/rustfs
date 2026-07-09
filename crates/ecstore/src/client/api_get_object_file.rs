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

use std::io;
use std::path::{Path, PathBuf};

#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

use crate::client::{
    api_error_response::err_invalid_argument, api_get_options::GetObjectOptions, transition_api::TransitionClient,
};

async fn prepare_download_target(file_path: &Path) -> io::Result<()> {
    match fs::metadata(file_path).await {
        Ok(metadata) if metadata.is_dir() => {
            return Err(io::Error::other(err_invalid_argument("filename is a directory.")));
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    if let Some(parent) = file_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).await?;

        #[cfg(not(windows))]
        {
            let mut permissions = fs::metadata(parent).await?.permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(parent, permissions).await?;
        }
    }

    Ok(())
}

fn build_part_path(file_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.part.rustfs", file_path.display()))
}

async fn open_download_part_file(file_part_path: &Path) -> io::Result<tokio::fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);

    #[cfg(not(windows))]
    options.mode(0o600);

    options.open(file_part_path).await
}

async fn cleanup_part_file(file_part_path: &Path) {
    let _ = fs::remove_file(file_part_path).await;
}

impl TransitionClient {
    pub async fn fget_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        file_path: &str,
        mut opts: GetObjectOptions,
    ) -> Result<(), io::Error> {
        let file_path = Path::new(file_path);
        prepare_download_target(file_path).await?;

        let file_part_path = build_part_path(file_path);
        let mut file_part = open_download_part_file(&file_part_path).await?;
        let existing_len = file_part.metadata().await?.len();
        if existing_len > 0 {
            opts.set_range(existing_len as i64, 0)?;
            file_part.seek(SeekFrom::Start(existing_len)).await?;
        }

        let (_object_info, _headers, mut object_reader) = self.get_object_inner(bucket_name, object_name, &opts).await?;
        if let Err(err) = tokio::io::copy(&mut object_reader, &mut file_part).await {
            cleanup_part_file(&file_part_path).await;
            return Err(err);
        }

        if let Err(err) = file_part.flush().await {
            cleanup_part_file(&file_part_path).await;
            return Err(err);
        }
        drop(file_part);

        if let Err(err) = fs::rename(&file_part_path, file_path).await {
            cleanup_part_file(&file_part_path).await;
            return Err(err);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn prepare_download_target_allows_missing_file_and_creates_parent_dirs() {
        let dir = tempdir().expect("temp dir");
        let target = dir.path().join("nested").join("object.bin");

        prepare_download_target(&target)
            .await
            .expect("missing target should be accepted");

        assert!(target.parent().expect("parent").exists(), "parent directory should be created");
        assert!(
            fs::metadata(&target).await.is_err(),
            "preparing the target should not create the final file eagerly"
        );
    }

    #[tokio::test]
    async fn prepare_download_target_rejects_directory_paths() {
        let dir = tempdir().expect("temp dir");
        let target_dir = dir.path().join("download-dir");
        fs::create_dir_all(&target_dir).await.expect("target dir");

        let err = prepare_download_target(&target_dir)
            .await
            .expect_err("directory targets must be rejected");

        assert!(err.to_string().contains("directory"), "unexpected error for directory target: {err}");
    }

    #[tokio::test]
    async fn open_download_part_file_creates_part_file() {
        let dir = tempdir().expect("temp dir");
        let target = dir.path().join("object.bin");
        let part_path = build_part_path(&target);

        let file = open_download_part_file(&part_path)
            .await
            .expect("part file should be created");
        drop(file);

        assert!(part_path.exists(), "part file should exist after creation");
    }
}
